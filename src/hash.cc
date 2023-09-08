/*
 * Copyright (c) 2023-present, Qihoo, Inc.  All rights reserved.
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#include "hash.h"
#include <cassert>
#include "store.h"

namespace pikiwidb {

PObject PObject::CreateHash() {
  PObject obj(PType_hash);
  obj.Reset(new PHash);
  return obj;
}

#define GET_HASH(hashname)                                         \
  PObject* value;                                                  \
  PError err = PSTORE.GetValueByType(hashname, value, PType_hash); \
  if (err != PError_ok) {                                          \
    ReplyError(err, reply);                                        \
    return err;                                                    \
  }

#define GET_OR_SET_HASH(hashname)                                  \
  PObject* value;                                                  \
  PError err = PSTORE.GetValueByType(hashname, value, PType_hash); \
  if (err != PError_ok && err != PError_notExist) {                \
    ReplyError(err, reply);                                        \
    return err;                                                    \
  }                                                                \
  if (err == PError_notExist) {                                    \
    value = PSTORE.SetValue(hashname, PObject::CreateHash());      \
  }

#if defined(USING_TBB)

void _set_hash_force(PHash& hash, const PString& key, const PString& val, Accessor& a) {
  const auto ok = hash.find(a, key);
  if (ok) {
    a->second = val;
  } else {
    const auto ok1 = hash.insert(a, PHash::value_type(key, val));
    assert(ok1);
  }
}

#else

PHash::iterator _set_hash_force(PHash& hash, const PString& key, const PString& val) {
  PHash::iterator it;
  it = hash.find(key);
  if (it != hash.end()) {
    it->second = val;
  } else {
    it = hash.insert(PHash::value_type(key, val)).first;
  }

  return it;
}

#endif

bool _set_hash_if_notexist(PHash& hash, const PString& key, const PString& val) {
#if defined(USING_TBB)
  return hash.insert(PHash::value_type(key, val));
#else
  return hash.insert(PHash::value_type(key, val)).second;
#endif
}

PError hset(const std::vector<PString>& params, UnboundedBuffer* reply) {
  GET_OR_SET_HASH(params[1]);

  auto hash = value->CastHash();
#if defined(USING_TBB)
  Accessor a;
  _set_hash_force(*hash, params[2], params[3], a);
#elif
  _set_hash_force(*hash, params[2], params[3]);
#endif

  FormatInt(1, reply);
  return PError_ok;
}

PError hmset(const std::vector<PString>& params, UnboundedBuffer* reply) {
  if (params.size() % 2 != 0) {
    ReplyError(PError_param, reply);
    return PError_param;
  }

  GET_OR_SET_HASH(params[1]);

  auto hash = value->CastHash();
  for (size_t i = 2; i < params.size(); i += 2) {
#if defined(USING_TBB)
    Accessor a;
    _set_hash_force(*hash, params[i], params[i + 1], a);
#else
    _set_hash_force(*hash, params[i], params[i + 1]);
#endif
  }

  FormatOK(reply);
  return PError_ok;
}

PError hget(const std::vector<PString>& params, UnboundedBuffer* reply) {
  GET_HASH(params[1]);

  auto hash = value->CastHash();
#if defined(USING_TBB)
  Accessor a;
  if (hash->find(a, params[2])) {
    FormatBulk(a->second, reply);
  } else {
    FormatNull(reply);
  }
#else
  auto it = hash->find(params[2]);

  if (it != hash->end()) {
    FormatBulk(it->second, reply);
  } else {
    FormatNull(reply);
  }
#endif

  return PError_ok;
}

PError hmget(const std::vector<PString>& params, UnboundedBuffer* reply) {
  GET_HASH(params[1]);

  PreFormatMultiBulk(params.size() - 2, reply);

  auto hash = value->CastHash();
  for (size_t i = 2; i < params.size(); ++i) {
#if defined(USING_TBB)
    Accessor a;
    if (hash->find(a, params[i])) {
      FormatBulk(a->second, reply);
    } else {
      FormatNull(reply);
    }
#else
    auto it = hash->find(params[i]);
    if (it != hash->end()) {
      FormatBulk(it->second, reply);
    } else {
      FormatNull(reply);
    }
#endif
  }

  return PError_ok;
}

PError hgetall(const std::vector<PString>& params, UnboundedBuffer* reply) {
  GET_HASH(params[1]);

  auto hash = value->CastHash();
  PreFormatMultiBulk(2 * hash->size(), reply);

  for (const auto& kv : *hash) {
    FormatBulk(kv.first, reply);
    FormatBulk(kv.second, reply);
  }

  return PError_ok;
}

PError hkeys(const std::vector<PString>& params, UnboundedBuffer* reply) {
  GET_HASH(params[1]);

  auto hash = value->CastHash();
  PreFormatMultiBulk(hash->size(), reply);

  for (const auto& kv : *hash) {
    FormatBulk(kv.first, reply);
  }

  return PError_ok;
}

PError hvals(const std::vector<PString>& params, UnboundedBuffer* reply) {
  GET_HASH(params[1]);

  auto hash = value->CastHash();
  PreFormatMultiBulk(hash->size(), reply);

  for (const auto& kv : *hash) {
    FormatBulk(kv.second, reply);
  }

  return PError_ok;
}

PError hdel(const std::vector<PString>& params, UnboundedBuffer* reply) {
  PObject* value;
  PError err = PSTORE.GetValueByType(params[1], value, PType_hash);
  if (err != PError_ok) {
    ReplyError(err, reply);
    return err;
  }

  int del = 0;
  auto hash = value->CastHash();
  for (size_t i = 2; i < params.size(); ++i) {
#if defined(USING_TBB)
    if (hash->erase(params[i])) {
      ++del;
    }
#else
    auto it = hash->find(params[i]);

    if (it != hash->end()) {
      hash->erase(it);
      ++del;
    }
#endif
  }

  FormatInt(del, reply);
  return PError_ok;
}

PError hexists(const std::vector<PString>& params, UnboundedBuffer* reply) {
  GET_HASH(params[1]);

  auto hash = value->CastHash();
#if defined(USING_TBB)
  Accessor a;
  if (hash->find(a, params[2])) {
    FormatInt(1, reply);
  } else {
    FormatInt(0, reply);
  }
#else
  auto it = hash->find(params[2]);

  if (it != hash->end()) {
    FormatInt(1, reply);
  } else {
    FormatInt(0, reply);
  }
#endif

  return PError_ok;
}

PError hlen(const std::vector<PString>& params, UnboundedBuffer* reply) {
  GET_HASH(params[1]);

  auto hash = value->CastHash();
  FormatInt(hash->size(), reply);
  return PError_ok;
}

PError hincrby(const std::vector<PString>& params, UnboundedBuffer* reply) {
  GET_OR_SET_HASH(params[1]);

  auto hash = value->CastHash();
  long val = 0;
  PString* str = nullptr;

#if defined(USING_TBB)
  Accessor a;
  if (hash->find(a, params[2])) {
    str = &a->second;
    if (Strtol(str->c_str(), static_cast<int>(str->size()), &val)) {
      val += atoi(params[3].c_str());
    } else {
      ReplyError(PError_nan, reply);
      return PError_nan;
    }
  } else {
    val = atoi(params[3].c_str());
    _set_hash_force(*hash, params[2], "", a);
    str = &a->second;
  }

#else

  auto it(hash->find(params[2]));
  if (it != hash->end()) {
    str = &it->second;
    if (Strtol(str->c_str(), static_cast<int>(str->size()), &val)) {
      val += atoi(params[3].c_str());
    } else {
      ReplyError(PError_nan, reply);
      return PError_nan;
    }
  } else {
    val = atoi(params[3].c_str());
    auto it = _set_hash_force(*hash, params[2], "");
    str = &it->second;
  }
#endif

  char tmp[32];
  snprintf(tmp, sizeof tmp - 1, "%ld", val);
  *str = tmp;

  FormatInt(val, reply);
  return PError_ok;
}

PError hincrbyfloat(const std::vector<PString>& params, UnboundedBuffer* reply) {
  GET_OR_SET_HASH(params[1]);

  auto hash = value->CastHash();
  float val = 0;
  PString* str = 0;

#if defined(USING_TBB)

 Accessor a;
 if (hash->find(a, params[2])) {
   str = &a->second;
   if (Strtof(str->c_str(), static_cast<int>(str->size()), &val)) {
     val += atof(params[3].c_str());
   } else {
     ReplyError(PError_param, reply);
     return PError_param;
   }
 } else {
   val = atof(params[3].c_str());
   _set_hash_force(*hash, params[2], "", a);
   str = &a->second;
 }

#else

  auto it(hash->find(params[2]));
  if (it != hash->end()) {
    str = &it->second;
    if (Strtof(str->c_str(), static_cast<int>(str->size()), &val)) {
      val += atof(params[3].c_str());
    } else {
      ReplyError(PError_param, reply);
      return PError_param;
    }
  } else {
    val = atof(params[3].c_str());
    auto it = _set_hash_force(*hash, params[2], "");
    str = &it->second;
  }

#endif

  char tmp[32];
  snprintf(tmp, sizeof tmp - 1, "%f", val);
  *str = tmp;

  FormatBulk(*str, reply);
  return PError_ok;
}

PError hsetnx(const std::vector<PString>& params, UnboundedBuffer* reply) {
  GET_OR_SET_HASH(params[1]);

  auto hash = value->CastHash();
  if (_set_hash_if_notexist(*hash, params[2], params[3])) {
    FormatInt(1, reply);
  } else {
    FormatInt(0, reply);
  }

  return PError_ok;
}

PError hstrlen(const std::vector<PString>& params, UnboundedBuffer* reply) {
  PObject* value;
  PError err = PSTORE.GetValueByType(params[1], value, PType_hash);
  if (err != PError_ok) {
    Format0(reply);
    return err;
  }

  auto hash = value->CastHash();
#if defined(USING_TBB)
  Accessor a;
  if (!hash->find(a, params[2])) {
    Format0(reply);
  } else {
    FormatInt(static_cast<long>(a->second.size()), reply);
  }
#else
  auto it = hash->find(params[2]);
  if (it == hash->end()) {
    Format0(reply);
  } else {
    FormatInt(static_cast<long>(it->second.size()), reply);
  }
#endif

  return PError_ok;
}

#if defined(USING_TBB)

inline size_t HScanHashMember(const PHash& container, size_t cursor, size_t count, std::vector<PHash::const_iterator>& res) {
  if (cursor >= container.size()) {
    return 0;
  }

  auto idx = cursor;
  for (decltype(container.bucket_count()) bucket = 0; bucket < container.bucket_count(); ++bucket) {
    const auto bktSize = container.bucket_size(bucket);
    if (idx < bktSize) {
     // find the bucket;
     auto it = container.begin(bucket);
     while (idx > 0) {
       ++it;
       --idx;
     }

     size_t newCursor = cursor;
     auto end = container.end(bucket);
     while (res.size() < count && it != end) {
       ++newCursor;
       res.push_back(it++);

       if (it == end) {
         while (++bucket < container.bucket_count()) {
           if (container.bucket_size(bucket) > 0) {
             it = container.begin(bucket);
             end = container.end(bucket);
             break;
           }
         }

         if (bucket == container.bucket_count()) {
           return 0;
         }
       }
     }

     return newCursor;
    } else {
     idx -= bktSize;
    }
  }

  return 0;  // never here
}

#endif

size_t HScanKey(const PHash& hash, size_t cursor, size_t count, std::vector<PString>& res) {
  if (hash.empty()) {
    return 0;
  }

  size_t newCursor = 0;

#if defined(USING_TBB)

  std::vector<PHash::const_iterator> iters;
  newCursor = HScanHashMember(hash, cursor, count, iters);

#else

  std::vector<PHash::const_local_iterator> iters;
  newCursor = ScanHashMember(hash, cursor, count, iters);

#endif

  res.reserve(iters.size());
  for (auto it : iters) {
    res.push_back(it->first), res.push_back(it->second);
  }

  return newCursor;
}

}  // namespace pikiwidb
