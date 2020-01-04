/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */

package org.apache.storm.starter;

import java.util.HashMap;
import java.util.Map;
import org.apache.storm.starter.spout.RandomSentenceSpout;
import org.apache.storm.task.ShellBolt;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.ConfigurableTopology;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Base64;
 
import javax.crypto.Cipher;
import javax.crypto.spec.SecretKeySpec;


/**
 * This topology demonstrates Storm's stream groupings and multilang
 * capabilities.
 */
public class WordCountTopology extends ConfigurableTopology {
    private static Logger LOG = LoggerFactory.getLogger(WordCountTopology.class);
    private class AES {
    
            private SecretKeySpec secretKey;
            private byte[] key;
        
            public void setKey(String myKey) 
            {
                MessageDigest sha = null;
                try {
                    key = myKey.getBytes("UTF-8");
                    sha = MessageDigest.getInstance("SHA-1");
                    key = sha.digest(key);
                    key = Arrays.copyOf(key, 16); 
                    secretKey = new SecretKeySpec(key, "AES");
                } 
                catch (NoSuchAlgorithmException e) {
                    e.printStackTrace();
                } 
                catch (UnsupportedEncodingException e) {
                    e.printStackTrace();
                }
            }
        
            public String encrypt(String strToEncrypt, String secret) 
            {
                try
                {
                    setKey(secret);
                    Cipher cipher = Cipher.getInstance("AES/ECB/PKCS5Padding");
                    cipher.init(Cipher.ENCRYPT_MODE, secretKey);
                    return Base64.getEncoder().encodeToString(cipher.doFinal(strToEncrypt.getBytes("UTF-8")));
                } 
                catch (Exception e) 
                {
                    //return "Error while encrypting";
                }
                return null;
            }
        
            public String decrypt(String strToDecrypt, String secret) 
            {
                try
                {
                    setKey(secret);
                    Cipher cipher = Cipher.getInstance("AES/ECB/PKCS5PADDING");
                    cipher.init(Cipher.DECRYPT_MODE, secretKey);
                    return new String(cipher.doFinal(Base64.getDecoder().decode(strToDecrypt)));
                } 
                catch (Exception e) 
                {
                    //return "Error while decrypting";
                }
                return null;
            }
    }

    public static void main(String[] args) throws Exception {
        ConfigurableTopology.start(new WordCountTopology(), args);
    }

    @Override
    protected int run(String[] args) throws Exception {

        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("spout", new RandomSentenceSpout(), 5);

        builder.setBolt("split", new SplitSentence(), 8).shuffleGrouping("spout");
        builder.setBolt("count", new WordCount(), 12).fieldsGrouping("split", new Fields("word"));

        conf.setDebug(true);

        String topologyName = "word-count";

        conf.setNumWorkers(3);

        if (args != null && args.length > 0) {
            topologyName = args[0];
        }
        return submit(topologyName, conf, builder);
    }

    public static class SplitSentence extends ShellBolt implements IRichBolt {

        public SplitSentence() {
            super("python", "splitsentence.py");
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("word"));
        }

        @Override
        public Map<String, Object> getComponentConfiguration() {
            return null;
        }
    }

    public static class WordCount extends BaseBasicBolt {
        Map<String, Integer> counts = new HashMap<String, Integer>();
        private AES aes;

        @Override
        public void execute(Tuple tuple, BasicOutputCollector collector) {
            String secretKey = "stormkey";
            String word = tuple.getString(0);
            Integer count = counts.get(word);
            if (count == null) {
                count = 0;
            }
            count++;
            counts.put(word, count);
            LOG.info("Count of word: " + word + " = " + count);
            String countStr = Integer.toString(count); 
            String encryptedString = aes.encrypt(countStr, secretKey) ;
            LOG.info("Encryption: " + countStr + " = " + encryptedString);
            
            
            collector.emit(new Values(word, encryptedString));
            //collector.emit(new Values(word, count));
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("word", "count"));
        }
    }
}
