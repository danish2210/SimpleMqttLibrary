#ifndef ___SIMPLE_MQTT_H_
#define ___SIMPLE_MQTT_H_

//#define DEBUG_PRINTS

const char mesh_gw_name[] = "m";

#define MQTT_MSG_ID_LIST_SIZE   30

#include <map>
#include <list>
#include <Arduino.h>
#include <safememcpy.h>

typedef enum
{
  SUBSCRIBE,
  UNSUBSCRIBE,
  GET,
  PUBLISH
} Mqtt_cmd;

typedef enum
{
  SWITCH_ON,
  SWITCH_OFF
} MQTT_switch;

typedef enum
{
  TRIGGERED
} MQTT_trigger;

typedef enum
{
  CONTACT_OPEN,
  CONTACT_CLOSED
} MQTT_contact;

typedef enum
{
  SHUTTER_OPEN,
  SHUTTER_CLOSE,
  SHUTTER_STOP
} MQTT_shutter;

typedef enum
{
  SET,
  VALUE,
  EITHER
} MQTT_IF;

typedef enum
{
  MODE_NODE_STD,
  MODE_NODE_RECEIVE_ALL,
  MODE_GW_ACK_ALL,
  MODE_GW_ACK_MY
} OP_MODE;


#pragma pack(push, 1)
struct mqtt_msgid_item {
  uint8_t mdsid[4];
  // unsigned long lastseen;
};
#pragma pack(pop)


// message example
// MQTT src_node/mUID
// P:dest_node/type/name/value message
// S:dest_node/type/name/value


class SimpleMQTT
{
public:
  SimpleMQTT(int ttl, const char *myDeviceName, uint16_t tryCount = 7, int timeoutMs = 200, uint16_t backoffMs = 70);
  ~SimpleMQTT();
  void setTimeouts(uint16_t tryCount, int timeoutMs, uint16_t backoffMs);
  void set_op_mode(OP_MODE mode = MODE_NODE_STD);
  void gen_random_str(char *s, const int len);
  char *get_msg_uuid(void);
  bool publish(const char *deviceName, const char *parameterName, const char *value);

  bool getTopic(const char *devName, const char *valName);
  bool subscribeTopic(const char *devName, const char *valName);
  bool unsubscribeTopic(const char *devName, const char *valName);

  void parse(const unsigned char *data, int size, uint32_t replyId);

  void handleEvents(void (*cb)(const char*, const char *, char , const char *, const char *));
  void handleEvents_raw(void (*cb)(const uint8_t *data, int len, uint32_t replyId));

  bool compareTopic(const char *topic, const char *deviceName, const char *t);

  bool _switch(Mqtt_cmd cmd, const char *name, MQTT_switch value = SWITCH_ON);
  bool _temp(Mqtt_cmd cmd, const char *name, float value = 0);
  bool _humidity(Mqtt_cmd cmd, const char *name, float value = 0);
  bool _trigger(Mqtt_cmd cmd, const char *name, MQTT_trigger value = TRIGGERED);
  bool _contact(Mqtt_cmd cmd, const char *name, MQTT_contact value = CONTACT_OPEN);
  bool _dimmer(Mqtt_cmd cmd, const char *name, uint8_t value = 0);
  bool _string(Mqtt_cmd cmd, const char *name, const char *value = NULL);
  bool _number(Mqtt_cmd cmd, const char *name, int min = 0, int max = 0, int step = 0);
  bool _float(Mqtt_cmd cmd, const char *name, float value = 0);
  bool _int(Mqtt_cmd cmd, const char *name, int value = 0);
  bool _shutter(Mqtt_cmd cmd, const char *name, MQTT_shutter value = SHUTTER_OPEN);
  bool _counter(Mqtt_cmd cmd, const char *name, int value = 0);
  bool _bin(Mqtt_cmd cmd, const char *name, const uint8_t *data = 0, int len = 0);

  bool _switch(Mqtt_cmd cmd, const std::list<const char *> &names, MQTT_switch value = SWITCH_ON);
  bool _temp(Mqtt_cmd cmd, const std::list<const char *> &names, float value = 0);
  bool _humidity(Mqtt_cmd cmd, const std::list<const char *> &names, float value = 0);
  bool _trigger(Mqtt_cmd cmd, const std::list<const char *> &names, MQTT_trigger value = TRIGGERED);
  bool _contact(Mqtt_cmd cmd, const std::list<const char *> &names, MQTT_contact value = CONTACT_OPEN);
  bool _dimmer(Mqtt_cmd cmd, const std::list<const char *> &names, uint8_t value = 0);
  bool _string(Mqtt_cmd cmd, const std::list<const char *> &names, const char *value = NULL);
  bool _number(Mqtt_cmd cmd, const std::list<const char *> &names, int min = 0, int max = 0, int step = 0);
  bool _float(Mqtt_cmd cmd, const std::list<const char *> &names, float value = 0);
  bool _int(Mqtt_cmd cmd, const std::list<const char *> &names, int value = 0);
  bool _shutter(Mqtt_cmd cmd, const std::list<const char *> &names, MQTT_shutter value = SHUTTER_OPEN);
  bool _counter(Mqtt_cmd cmd, const std::list<const char *> &names, int value = 0);
  bool _bin(Mqtt_cmd cmd, const std::list<const char *> &names, const uint8_t *data = 0, int len = 0);

  bool _ifSwitch(MQTT_IF ifType, const char *name, void (*cb)(MQTT_switch /*value*/));
  bool _ifTemp(MQTT_IF ifType, const char *name, void (*cb)(float /*value*/));
  bool _ifHumidity(MQTT_IF ifType, const char *name, void (*cb)(float /*value*/));
  bool _ifTrigger(MQTT_IF ifType, const char *name, void (*cb)(MQTT_trigger /*value*/));
  bool _ifContact(MQTT_IF ifType, const char *name, void (*cb)(MQTT_contact /*value*/));
  bool _ifDimmer(MQTT_IF ifType, const char *name, void (*cb)(uint8_t /*value*/));
  bool _ifString(MQTT_IF ifType, const char *name, void (*cb)(const char * /*value*/));
  bool _ifNumber(MQTT_IF ifType, const char *name, void (*cb)(int /*min*/, int /*max*/, int /*step*/));
  bool _ifFloat(MQTT_IF ifType, const char *name, void (*cb)(float /*value*/));
  bool _ifInt(MQTT_IF ifType, const char *name, void (*cb)(int /*value*/));
  bool _ifShutter(MQTT_IF ifType, const char *name, void (*cb)(MQTT_shutter /*value*/));
  bool _ifCounter(MQTT_IF ifType, const char *name, void (*cb)(int /*value*/));
  bool _ifBin(MQTT_IF ifType, const char *name, void (*cb)(const uint8_t * /*bin*/, int /*length*/));

  bool send(const char *mqttMsg, int len, uint32_t replyId);

private:
  String myDeviceName;
  char buffer[250];
  uint32_t replyId;
  OP_MODE op_mode = MODE_NODE_STD;
  bool _raw(Mqtt_cmd cmd, const char *type, const std::list<const char *> &names, const char *value);
  bool _rawIf(MQTT_IF ifType, const char *type, const char *name);
  void (*publishCallBack)(const char* src_node_name, const char *msgid, char command, const char *topic, const char *value);
  void (*rawCallBack)(const uint8_t *data, int len, uint32_t replyId);

  void parse2(const char *c, unsigned int l, char* src_node_name, char *msgid, bool new_msg);
  bool compare(MQTT_IF ifType, const char *type, const char *name);

  const char *decompressTopic(const char *topic);

  int ttl;
  uint16_t tryCount;
  int timeoutMs;
  uint16_t backoffMs;

  const char *_topic;
  const char *_value;

};
#endif
