--
-- Copyright (c) 2013, Evan Wies <evan at neomantra dot net>
-- All rights reserved.
--
-- Originates from work by:
-- Copyright (c) 2009-2012, Salvatore Sanfilippo <antirez at gmail dot com>
--
-- Redistribution and use in source and binary forms, with or without
-- modification, are permitted provided that the following conditions are met:
--
--   * Redistributions of source code must retain the above copyright notice,
--     this list of conditions and the following disclaimer.
--   * Redistributions in binary form must reproduce the above copyright
--     notice, this list of conditions and the following disclaimer in the
--     documentation and/or other materials provided with the distribution.
--   * Neither the name of Redis nor the names of its contributors may be used
--     to endorse or promote products derived from this software without
--     specific prior written permission.
--
-- THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
-- AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
-- IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
-- ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
-- LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
-- CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
-- SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
-- INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
-- CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
-- ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
-- POSSIBILITY OF SUCH DAMAGE.
--

-- Redis Lua Script support
--
-- WISHLIST
--   sds in Lua
--   redis.call_raw and redis.pcall_raw which take/return buffers (for zero-copy)
--   redis.call and redis.pcall in Lua
--

local ffi = require 'ffi'

local RC = ffi.cdef([[
typedef struct redisObject {
    unsigned type:4;
    unsigned notused:2;     /* Not used */
    unsigned encoding:4;
    unsigned lru:22;        /* lru time (relative to server.lruclock) */
    int refcount;
    void *ptr;
} robj;

typedef char *sds;

struct sdshdr {
    int len;
    int free;
    char buf[];
};

sds sdsnewlen(const void *init, size_t initlen);
sds sdsnew(const char *init);
sds sdsempty();
size_t sdslen(const sds s);
sds sdsdup(const sds s);
void sdsfree(sds s);
size_t sdsavail(const sds s);
sds sdsgrowzero(sds s, size_t len);
sds sdscatlen(sds s, const void *t, size_t len);
sds sdscat(sds s, const char *t);
sds sdscatsds(sds s, const sds t);
sds sdscpylen(sds s, const char *t, size_t len);
sds sdscpy(sds s, const char *t);

sds sdstrim(sds s, const char *cset);
sds sdsrange(sds s, int start, int end);
void sdsupdatelen(sds s);
void sdsclear(sds s);
int sdscmp(const sds s1, const sds s2);
sds *sdssplitlen(const char *s, int len, const char *sep, int seplen, int *count);
void sdsfreesplitres(sds *tokens, int count);
void sdstolower(sds s);
void sdstoupper(sds s);
sds sdsfromlonglong(long long value);
sds sdscatrepr(sds s, const char *p, size_t len);
sds *sdssplitargs(const char *line, int *argc);
void sdssplitargs_free(sds *argv, int argc);
sds sdsmapchars(sds s, const char *from, const char *to, size_t setlen);

sds sdscatvprintf(sds s, const char *fmt, va_list ap);
sds sdscatprintf(sds s, const char *fmt, ...);
size_t sdslen(const sds s);
size_t sdsavail(const sds s);

enum {
    REDIS_SHARED_SELECT_CMDS = 10,
    REDIS_SHARED_INTEGERS = 10000,
    REDIS_SHARED_BULKHDR_LEN = 32
};

struct sharedObjectsStruct {
    robj *crlf, *ok, *err, *emptybulk, *czero, *cone, *cnegone, *pong, *space,
    *colon, *nullbulk, *nullmultibulk, *queued,
    *emptymultibulk, *wrongtypeerr, *nokeyerr, *syntaxerr, *sameobjecterr,
    *outofrangeerr, *noscripterr, *loadingerr, *slowscripterr, *bgsaveerr,
    *masterdownerr, *roslaveerr, *execaborterr,
    *oomerr, *plus, *messagebulk, *pmessagebulk, *subscribebulk,
    *unsubscribebulk, *psubscribebulk, *punsubscribebulk, *del, *rpop, *lpop,
    *lpush,
    *select[REDIS_SHARED_SELECT_CMDS],
    *integers[REDIS_SHARED_INTEGERS],
    *mbulkhdr[REDIS_SHARED_BULKHDR_LEN], /* "*<value>\r\n" */
    *bulkhdr[REDIS_SHARED_BULKHDR_LEN];  /* "$<value>\r\n" */
};

extern struct sharedObjectsStruct shared;

typedef struct redisClient redisClient;
typedef struct lua_State lua_State;

void _redisAssert(char *estr, char *file, int line);

void addReply(redisClient *c, robj *obj);
void addReplyBulkCBuffer(redisClient *c, void *p, size_t len);
void addReplyLongLong(redisClient *c, long long ll);
void addReplyErrorFormat(redisClient *c, const char *fmt, ...);
void *addDeferredMultiBulkLength(redisClient *c);
void setDeferredMultiBulkLength(redisClient *c, void *node, long length);
void addReplySds(redisClient *c, sds s);

int luaCreateFunction(redisClient *c, lua_State *lua, char *funcname, robj *body);

typedef struct scriptBridge {
    robj** argv;
    int argc;
    int numkeys;
    redisClient* c;
    lua_State* lua;
} sb;

const sb* ljr_getScriptBridge();
void ljr_redirectLuaCaller( redisClient* c );


]])

local ffi_string, ffi_cast, C = ffi.string, ffi.cast, ffi.C
local char_ptr_t = ffi.typeof('char*')
local const_char_ptr_t = ffi.typeof('const char*')
local sdshdr_ptr_t = ffi.typeof('struct sdshdr*')
local sdshdr_sz = ffi.sizeof('struct sdshdr')

local REDIS_ERR = -1


local function robj2string(robj)
   local ptr = ffi_cast(char_ptr_t, robj.ptr)
   local len = ffi_cast(sdshdr_ptr_t, ptr-sdshdr_sz).len
   return ffi_string(ptr, len)
end


local function redis_get_keys_argv( sb )
    local KEYS, ARGV = {}, {}
    local elev = sb.argv+3
    for i = 0, sb.numkeys-1 do KEYS[#KEYS+1] = robj2string(elev[i]) end
    elev = sb.argv + 3 + sb.numkeys
    for i = 0, sb.argc-3-sb.numkeys-1 do ARGV[#ARGV+1] = robj2string(elev[i]) end
    return KEYS, ARGV
end


-- Add a helper funciton that we use to sort the multi bulk output of non
-- deterministic commands, when containing 'false' elements.
function __redis__compare_helper(a,b)
    if a == false then a = '' end
    if b == false then b = '' end
    return a < b
end

--replyToRedisReply
function replyToRedisReply( c, val )
    local t = type(val)
    if t == 'string' then
        C.addReplyBulkCBuffer(c, ffi.cast(char_ptr_t, val), #val)
    elseif t == 'boolean' then
        C.addReply(c, val and C.shared.cone or C.shared.nullbulk)
    elseif t == 'number' then
        C.addReplyLongLong(c, val)
    elseif t == 'table' then
        -- We need to check if it is an array, an error, or a status reply.
        -- Error are returned as a single element table with 'err' field.
        -- Status replies are returned as single elment table with 'ok' field 
        if type(val.err) == 'string' then
            local err = C.sdsnew(val.err)
            C.sdsmapchars(err,'\r\n','  ',2)
            C.addReplySds(c,C.sdscatprintf(C.sdsempty(),"-%s\r\n",err))
            C.sdsfree(err)
        elseif type(val.ok) == 'string' then
            local ok = C.sdsnew(val.ok)
            C.sdsmapchars(ok,'\r\n','  ',2)
            C.addReplySds(c,C.sdscatprintf(C.sdsempty(),"+%s\r\n",ok))
            C.sdsfree(ok)
        else
            local replylen = C.addDeferredMultiBulkLength(c)
            local j, mbulklen = 1, 0

            while true do
                local v = val[j]
                if type(v) == 'nil' then break end
                replyToRedisReply(c, v)
                j = j + 1
                mbulklen = mbulklen + 1
            end
            C.setDeferredMultiBulkLength(c, replylen, mbulklen)
        end
    else
        C.addReply(c, C.shared.nullbulk)
    end
end


local function sandbox_randomseed( seed )
    if math and math.randomseed then
        math.randomseed( seed )
    end
end

-- Global table which maps SHA hashes to Lua functions
sha_funcs = {}

local first_eval_command = true
local sb = C.ljr_getScriptBridge()

function evalGenericCommand_Lua( funcname, evalsha )

    -- setup globals for the first invocation of this function
    if first_eval_command then
        setfenv( sandbox_randomseed, redis_fenv )
        first_eval_command = false
    end

    -- Try to lookup the Lua function
    local fn = sha_funcs[funcname]
    if type(fn) == 'nil' then
        -- Function not defined... let's define it if we have the
        -- body of the funciton. If this is an EVALSHA call we can just
        -- return an error.
        if evalsha then
            C.addReply( sb.c, C.shared.noscripterr )
            return true  -- error returned
        end

        local res = C.luaCreateFunction( sb.c, sb.lua, ffi.cast(char_ptr_t,funcname), sb.argv[1] )
        if res == REDIS_ERR then return true end -- error returned
        -- Now the following is guaranteed to be non nil 
        fn = sha_funcs[funcname]
        if type(fn) == 'nil' then _redisAssert("sha_funcs[funcname] was nil", 0, 0) end
    end

    -- We want the same PRNG sequence at every call so that our PRNG is not
    -- affected by external state. So math.randomseed(0) is called at the start
    -- This is executed in the redis_fenv environment
    sandbox_randomseed( 0 )

    -- Switch to sandbox function environment
    -- The redis_fenv global was set in scriptingInit
    local KEYS, ARGV = redis_get_keys_argv( sb )
    local success, res = pcall( fn, KEYS, ARGV )

    C.ljr_redirectLuaCaller( sb.c )
    if success then
        -- On success convert the Lua return value into Redis protocol, and
        -- send it to * the client.
        replyToRedisReply( sb.c, res )
    else
        C.addReplyErrorFormat( sb.c, "Error running script (call to %s): %s\n",
            funcname, tostring(res) )
    end
end



------------------------------------------------------------------------------
-- Redis Scripting API
-- 
-- Defined here:
--     http://redis.io/commands/eval
--
-- These are only in the final function environment, if included in redis_env.lua
--


-- Global table to hold Redis Scripting API
--redis = {}
--TODO: redis global table is first established in C, for the next line
--TODO: redis.call, redis.pcall, and redis.sha1hex are in C and injected there


function redis.error_reply( error_string )
    return { err = tostring(error_string) }
end

function redis.status_reply( status_string )
    return { ok = tostring(status_string) }
end

redis.LOG_DEBUG   = 0
redis.LOG_VERBOSE = 1
redis.LOG_NOTICE  = 2
redis.LOG_WARNING = 3


function redis.log( ... )
    local argc = select('#', ...)
    local level = select(1, ...)

    if argc < 2 then
        return nil, 'redis.log() requires two arguments or more.'
    elseif type(level) ~= 'number' then
        return nil, 'First argument must be a number (log level).'
    end

    level = tonumber(LEVEL)
    if level < redis.LOG_DEBUG or level > redis.LOG_WARNING then
        return nil, 'Invalid debug level.'
    end

    -- Glue together all the arguments
    local log = C.sdsempty()
    local j = 2 ; while j <= argc do
        local s = tostring(select(j, ...))
        local len = #s
        if s then
            log = C.sdscatlen(log,' ',1)
            log = C.sdscatlen(log,s,len)
        end
        j = j + 1
    end

    C.redisLogRaw(level,log)
    C.sdsfree(log)
    return true
end


----- The following are additions to the Redis Scripting API

-- makeReadOnly( t )
--
-- Prevents creating new elements in the given table.
-- Since this table will be the function environment,
-- it effectively prevents the use of global variables.
--
-- This should generally be the last step in setting up the environment,
-- as you won't be able to modify it anymore.
--
-- Note that this specific behavior is different than stock Redis.
-- 
function redis.makeTableReadOnly( t )
    t = t or {}
    local mt = {
        __index = function( t, k )
            return rawget(t, n)
        end,
        __newindex = function( t, k, v )
            error('Script attempt to update a read-only table with "'..tostring(k)..'"')
        end
    }
    setmetatable(t, mt)
    return t
end

