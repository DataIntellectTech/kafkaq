
/ initconsumer[server;optiondict]
/ initialise consumer object with the specified config options. Required in order to call 'subscribe'
/ e.g. initconsumer[`fetch.wait.max.ms`fetch.error.backoff.ms!`5`5]
initconsumer:`:./kafkaq 2: (`initconsumer;2)

/ initproducer[server;optiondict]
/ e.g. initproducer[`localhost:9092;`queue.buffering.max.ms`batch.num.messages!`5`1]
initproducer:`:./kafkaq 2: (`initproducer;2)

/ cleanupconsumer[]
/ disconnect and free up consumer object, stop and subscription thread
cleanupconsumer:`:./kafkaq 2: (`cleanupconsumer;1)

/ cleanupproducer[]
/ disconnect and free up producer object
cleanupproducer:`:./kafkaq 2: (`cleanupproducer;1)

/ subscribe[topic;partition]
/ start subscription thread for topic on partition - data entry point is 'kupd' function
/ e.g. subscribe[`test;0]
subscribe:`:./kafkaq 2: (`subscribe;2)

/ publish[topic;partition;key;message]
/ publish 'message' byte vector to topic, partition. symbol key can be null
/ e.g. publish[`test;0;`;`byte$"hello world"]
publish:`:./kafkaq 2: (`publish;4)

/ default entry point - if subscription is active this will be called with any messages 
/ k (symbol) - key
/ x (bytes) - message content
kupd:{[k;x] -1 `char$x;}

