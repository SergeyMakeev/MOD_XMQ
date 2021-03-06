## About

ejabberd module to support **XMQ** protocol.
Compile using erlang shell command c(mod_xmq).

**XMQ** is a custom extension for XMPP protocol to provide message broker features.
Supports two types of queues:

- **ALL**. The message will receive by all clients subscribing to specified topic.
  Similar to [XEP-0060: Publish-Subscribe](https://xmpp.org/extensions/xep-0060.html)         
- **ANY**. The message will receive by any client subscribing to specified topic.
  Similar to [XEP-0354: Customizable Message Routing](https://xmpp.org/extensions/xep-0354.html) 


Here is the simple example of configuration file ejabberd.yml
```
modules:
  mod_xmq:
    check_expires_time: 50
    queues:
      - 
        route: "queue1.localhost"
        type: any
      - 
        route: "queue2.localhost"
        type: all
```

**XMQ** use custom presence messages to track active clients.

Example of a subscription command.

- **ttl** Specifies the lifetime of the subscription in seconds.
  The subscription will automatically expire according to the ttl parameter.
- **load** Specifies the load of current client. In the current implementation, the client does not receive any messages from queue if its load factor is greater than 90. Useful for load balancing in future.
```XML
<iq type='set'
    id='id1'
    to='queue1.localhost'
    from='user@sm-localhost/desktop' >
  <xmq xmlns='xmq:command'
       command='sub'
       topic='topic'
       ttl='60'
       load='1'/>
</iq>
```


Example of a command to delete a subscription.
```XML
<iq type='set'
    id='id2'
    to='queue1.localhost'
    from='user@sm-localhost/desktop' >
  <xmq xmlns='xmq:command'
       command='del'
       topic='topic'/>
</iq>
```

Example of Message with specified **XMQ** topic.
```XML
<message type='chat'
         to='queue1.localhost'
         from='user@sm-localhost/desktop' >
  <body>Hello world</body>
  <subject>Subj</subject>
  <thread>Thread</thread>
  <xmq xmlns='xmq:topic'
       topic='topic'/>
</message>
```

Example of IQ with specified **XMQ** topic.
```XML
<iq type='get'
    id='id3'
    to='queue1.localhost'
    from='user@sm-localhost/desktop' >
  <custom_query xmlns='::cusom_xml_ns'>some custom data</custom_query>
  <xmq xmlns='xmq:topic'
       topic='topic'/>
</iq>
```
