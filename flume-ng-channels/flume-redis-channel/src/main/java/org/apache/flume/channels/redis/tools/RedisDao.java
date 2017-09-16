/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.flume.channels.redis.tools;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;
import redis.clients.jedis.Transaction;

public class RedisDao {
    private static final Logger log = LoggerFactory.getLogger(RedisDao.class);
    private static RedisDao instance;
    private static RedisController rc;

    private RedisDao(String host, int port, String passwd) {
        rc = RedisController.getRedisController(host, port, passwd);
    }

    public static synchronized RedisDao getInstance(String host, int port, String passwd) {
        if (null == instance) {
            instance = new RedisDao(host, port, passwd);
        }
        return instance;
    }
    
    
    public long lpush(String key,String... strings) {
        Jedis jedis = rc.getController();
        long count = 0;
        try {              	
            count = jedis.lpush(key, strings);
        } finally {
            jedis.close();
        }
        return count;
    }

    public String rpoplpush(String srckey, String dstkey) {
        Jedis jedis = rc.getController();
        String msg = "";
        try {
            msg = jedis.rpoplpush(srckey, dstkey);
        } finally {
            jedis.close();
        }
        return msg;
    }

    public String rpop(String key) {
        Jedis jedis = rc.getController();
        String msg = "";
        try {
            msg = jedis.rpop(key);
        } finally {
            jedis.close();
        }
        return msg;
    }

    public Long llen(String key) {
        Jedis jedis = rc.getController();
        long result;
        try {
            result = jedis.llen(key);
        } finally {
            jedis.close();
        }
        return result;
    }
    public List<String> mPop(String key, Long batch){
    	Jedis jedis = rc.getController();    	
    	Response<List<String>> result;
    	try{   		
    		Long llen = jedis.llen(key);
    		Long start = llen-batch;
    		if (start < 0L){
    			start = 0L;
    		}
    		Transaction transaction = jedis.multi();    		
    		result = transaction.lrange(key,start,-1);
    		transaction.ltrim(key, 0, start-1);
    		transaction.exec();
    		return result.get();
    	}finally{
    		jedis.close();
    	}
    }
    
    public Jedis getRedis() {
        return rc.getController();
    }

}
