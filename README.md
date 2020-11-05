## SimpleMQTTLibrary (Version2) for flooding mesh network
fork from: https://github.com/arttupii/SimpleMqttLibrary

Version 2 Features added:
- bugfixes
- async publish/subscribe 
- message delivery guarantee: repeat lost messages with timeout/backoff/repeat settings
- duplicate message cache, now we receive unique MQTT message only once
- secure random generator used
- new raw messages callback (see examples: TODO)
- resend message loop (must be called periodically)
- modified message format for more concise protocol
- main gateway node name is 'm' (DestinationDeviceName) (gateway to mqtt broker)
- new base64 library


### Protocol messages:
DestinationDeviceName is usually a mqtt gateway name 'm'

Example:
```
"MQTT SrcNodeName/MsgUUID"
P:DestinationNodeName/temp/bme280/value 23.54
"
```

#### Subscribe topic nodename/led/value
```
"MQTT nodename/MsgUUID"
S:m/led/value
"
```

#### Publish topic nodename/led/value
```
"MQTT nodename/MsgUUID"
P:m/temp/bme280/value 23.54
"
```
#### Multiple MQTT commands in the same message
```
"MQTT nodename/MsgUUID"
G:device1/switch/led/value
S:device1/switch/led/set
G:device1/switch/led1/value
S:device1/switch/led1/set
G:device1/temp/dallas1/value
G:device1/temp/dallas2/value
G:device2/switch/led/value
S:device2/switch/led/set
"
```
##### "Compressed" message
```
"MQTT nodename/MsgUUID"
G:device1/switch/led/value     -->Topic is device1/switch/led/value
S:.../set                      -->Topic is device1/switch/led/set
G:../led1/value                -->Topic is device1/switch/led1/value
S:.../set                      -->Topic is device1/switch/led1/set
G:./temp/dallas1/value         -->Topic is device1/temp/dallas1/value
G:../dallas2/value             -->Topic is device1/temp/dallas2/value
G:device2/switch/led/value     -->Topic is device2/switch/led/value
S:.../set                      -->Topic is device2/switch/led/set
"
```
