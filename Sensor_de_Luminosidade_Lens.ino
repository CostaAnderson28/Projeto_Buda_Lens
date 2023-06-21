#include <ESP8266WiFi.h>          

#include <PubSubClient.h>   

#include <NTPClient.h>

#include <WiFiUdp.h>

#define sensor A0

bool luz = false;

const char* ssid = "Costa"; 

const char* password = "28032002"; 

const char* mqtt_server = "test.mosquitto.org";

WiFiUDP ntpUDP;

NTPClient timeClient(ntpUDP, "south-america.pool.ntp.org");

WiFiClient espClient;

PubSubClient client(espClient);

unsigned long lastMsg = 0;

#define MSG_BUFFER_SIZE  (50)

char msg[MSG_BUFFER_SIZE];                   

int porta= 1883;

void setup_wifi() {

  delay(500);

  Serial.println();

  Serial.print("Conectando ao Wifi: ");

  Serial.println(ssid);

  WiFi.mode(WIFI_STA);

  WiFi.begin(ssid, password);

  while (WiFi.status() != WL_CONNECTED) {

    delay(500);

    Serial.print(".");

  }

  randomSeed(micros());

  Serial.println("");

  Serial.println("Conectado!");

  Serial.println("IP: ");

  Serial.println(WiFi.localIP());

}
void callback(char* topic, byte* payload, unsigned int length) {

  Serial.print("Mensagem Recebida no Topico [");

  Serial.print(topic);

  Serial.print("] ");

  for (int i = 0; i < length; i++) {

    Serial.print((char)payload[i]);
  
  }

  Serial.println();

}
void reconnect() {
  
  while (!client.connected()) {

  Serial.print("Tentando reconectar...");

  String clientId = "Luz-ESP8266-Client-";

  clientId += String(random(0xffff), HEX);

  if (client.connect(clientId.c_str())) {

    Serial.println("Conectado!");

    client.publish("Labnet/Luz", "hello world");

    client.subscribe("Labnet/Luz");

  } else {

    Serial.print("Falha ao conectar, rc=");

    Serial.print(client.state());

    delay(5000);

    }

  }

}
void setup() {

  Serial.begin(115200);

  setup_wifi();

  client.setServer(mqtt_server, 1883);

  client.setCallback(callback);

  timeClient.begin();

  timeClient.setTimeOffset(-10800);

}
void loop() {

  if (!client.connected()) {

    reconnect();

  }

  client.loop();

  timeClient.update();

  unsigned long now = millis();

  int value_luz = analogRead(sensor); 

  time_t epochTime = timeClient.getEpochTime();

  struct tm *ptm = gmtime ((time_t *)&epochTime); 

  int diaMes = ptm-> tm_mday;

  int atualMes = ptm-> tm_mon + 1;

  int atualAno = ptm-> tm_year + 1900;

  String atualData = String(atualAno) + "-" + String(atualMes) + "-" + String(diaMes);

  String formattedTime = timeClient.getFormattedTime();
    
  if (now - lastMsg > 2000) {

    lastMsg = now;

    if(value_luz>105){

      luz=true;

      snprintf (msg, MSG_BUFFER_SIZE, "Luz: %1d - [Horário: %s]- [Data: %s]", value_luz, formattedTime, atualData);

      client.publish("Labnet/Luz", msg);

      delay(2000);

    }
    else{

      luz=false;

      snprintf (msg, MSG_BUFFER_SIZE, "Luz: %1d - [Horário: %s]- [Data: %s]", value_luz, formattedTime, atualData);

      client.publish("Labnet/Luz", msg);

      delay(2000);

    }

  }
 
}
