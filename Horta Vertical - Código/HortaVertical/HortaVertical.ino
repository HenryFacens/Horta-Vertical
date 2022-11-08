/*
  O sistema é ligado para efetuar a irrigação e enquanto esta ligado fica mandando o status da valvula. Apos desligar passa mais 5 minutos enviando o status da valvula desligada
*/
#include <ArduinoJson.h> // Versão 5.12.0
#include <ESP8266WiFi.h>
#include <ESP8266HTTPClient.h>
#include <Wire.h>       //I2C library
#include <RtcDS3231.h>  //RTC library
#include <EEPROM.h>
#include <ESP8266mDNS.h>
#include <WiFiUdp.h>
#include <ArduinoOTA.h>

WiFiClient client;

// ============================================== RTC setup ================================================================================
RtcDS3231<TwoWire> rtcObject(Wire); //Uncomment for version 2.0.0 of the rtc library
HTTPClient http;

#define rele 14
#define ledEnvio 12

String payloadGlobal; //Essa variavel foi criada para conseguirmos acessar a variavel payload de qualquer lugar do codigo
int horas;
int minutos;
int v1;
int contador = 0;
String acessoRemoto = "0"; //Trocar acesso remoto por status somente
String acessoLocal = "0";


const char* ssid = "SCF-IOT";
const char* password = "Smart2017!";

void setup () {
  //================ Setup WIFI
  Serial.begin(115200); 
  WiFi.begin(ssid, password); // Inicia o wifi com as credenciais passadas acima

  //================ Tenta se conectar ao WIFI 60 vezes senão conectar inicia sem conexão com o WIFI
  while (WiFi.status() != WL_CONNECTED && contador < 60) {
    delay(1000);
    Serial.print("Connecting..");
    contador ++;
  }
  if (WiFi.status() == WL_CONNECTED) {
    Serial.print("Conectado");
  } else {
    Serial.print("Iniciando sem conexão com WIFI");
  }
  //================ Setup RTC =========================
  Serial.begin(115200);  //Starts serial connection
  rtcObject.Begin();     //Starts I2C
  //RtcDateTime currentTime = RtcDateTime(2020, 07, 06, 10, 24, 10); //define date and time object
  //rtcObject.SetDateTime(currentTime); //configure the RTC with object
  
  //==================================================================================================== Setup OTA ======

  // Port defaults to 8266
  // ArduinoOTA.setPort(8266);

  // Hostname defaults to esp8266-[ChipID]
  ArduinoOTA.setHostname("Horta Vertical 1 - V1");

  // No authentication by default
  // ArduinoOTA.setPassword((const char *)"Smart2017!");

  ArduinoOTA.onStart([]() {
    Serial.println("Start");
  });
  ArduinoOTA.onEnd([]() {
    Serial.println("\nEnd");
  });
  ArduinoOTA.onProgress([](unsigned int progress, unsigned int total) {
    Serial.printf("Progress: %u%%\r", (progress / (total / 100)));
  });
  ArduinoOTA.onError([](ota_error_t error) {
    Serial.printf("Error[%u]: ", error);
    if (error == OTA_AUTH_ERROR) Serial.println("Auth Failed");
    else if (error == OTA_BEGIN_ERROR) Serial.println("Begin Failed");
    else if (error == OTA_CONNECT_ERROR) Serial.println("Connect Failed");
    else if (error == OTA_RECEIVE_ERROR) Serial.println("Receive Failed");
    else if (error == OTA_END_ERROR) Serial.println("End Failed");
  });
  ArduinoOTA.begin();
  Serial.println("Ready");
  Serial.print("IP address: ");
  Serial.println(WiFi.localIP());

  //==================================================================================================== Setup Entradas e Saidas =======
  pinMode(ledEnvio, OUTPUT);
  pinMode(rele, OUTPUT);

  //==================================================================================================== Setup EEPROM =================
  EEPROM.begin(4);
  if(!EEPROM.read(0)&&!EEPROM.read(1)){
  EEPROM.write(0, 10);
  EEPROM.write(1, 11);
  EEPROM.commit();
  }

  Serial.print("Time0 EEPROM: ");
  Serial.println(EEPROM.read(0));

  Serial.print("Time1 EEPROM: ");
  Serial.println(EEPROM.read(1));
  atualizaHora();

}

void loop() {

  ConsultaHoraAtualRTC();
  Serial.println();
  Serial.print("Payload:");
  Serial.println(payloadGlobal);
  Serial.print("Acesso Remoto:");
  Serial.println(acessoRemoto);
   
  //Executa as funçoes que dependem de internet
  if (WiFi.status() == WL_CONNECTED) {
    ConsultaStatusBotaoAPI();
    ConsultaProgramacaoServidor();
    //Atualiza o RTC com horario do servidor a cada hora
    if (minutos == 0) atualizaHora();
  }
  
  // Se houver acionamento manual ou estiver no horario de ligar realiza a irrigação
  if (horas == 7 && minutos < EEPROM.read(0) || (acessoRemoto != payloadGlobal && payloadGlobal == "1") || horas == 17 && minutos < EEPROM.read(1)) {
    digitalWrite(rele, HIGH);
    v1 = 1;
    EnviaStatusValvulaAPI(); // Envia o status da valvula para o Thingspeak
    acessoRemoto = payloadGlobal; // Em caso de acesso manual garante que sera enviado apenas uma vez o status para o servidor
  } 
  // Se houver o desacionamento manual ou estiver no horario de desligar realiza a paralização da irrigação
  else if (horas == 7 && minutos >= EEPROM.read(0) && minutos < EEPROM.read(0) + 5 || (acessoRemoto != payloadGlobal && payloadGlobal == "0" ) || horas == 17 && minutos >= EEPROM.read(1) && minutos < EEPROM.read(1) + 5) {
    digitalWrite(rele, LOW);
    v1 = 0;
    EnviaStatusValvulaAPI(); // Envia o status da valvula para o Thingspeak
    acessoRemoto = payloadGlobal; // Em caso de acesso manual garante que sera enviado apenas uma vez o status para o servidor
  }

  //==================================================================================================== Caso perca a conexão ele tenta reconectar de novo
  if (WiFi.status() != WL_CONNECTED) {
    WiFi.begin(ssid, password);
    contador = 0;
    while (WiFi.status() != WL_CONNECTED && contador < 15) {
      delay(1000);
      Serial.print("Tentando Reconexão...");
      contador ++;
    }
  }
ArduinoOTA.handle();
}


//==================================================================================================== Pega Dado do fild1 do Thingspeak
void ConsultaStatusBotaoAPI() {
  if (WiFi.status() == WL_CONNECTED) { //Check WiFi connection status
    HTTPClient http;  //Declare an object of class HTTPClient

    http.begin("http://api.thingspeak.com/channels/1242882/feeds.json?api_key=AGO1CHUO5D9O9VYY&results=1"); //Specify request destination
    int httpCode = http.GET();                                                              //Send the request

    if (httpCode > 0) { //Check the returning code
      //########################################################################

      //========================================================================
      // Link para gerar o parsing Json: https://arduinojson.org/v5/assistant/
      //========================================================================

      //########################################################################

      //========================================================================
      // Exemplo de consulta de um field do projto Automação do Jardim Vertical
      //========================================================================

      const size_t capacity = JSON_ARRAY_SIZE(1) + JSON_OBJECT_SIZE(2) + JSON_OBJECT_SIZE(10) + JSON_OBJECT_SIZE(15) + 470;
      // DynamicJsonBuffer jsonBuffer(capacity);

      DynamicJsonDocument doc(capacity);

      //JsonObject& root = jsonBuffer.parseObject(http.getString());
      String jsonReturn = http.getString();
      deserializeJson(doc, jsonReturn);
      //feeds_0 = doc["feeds"][0];

      const char* payload = doc["feeds"][0]["field1"]; //Variavel global por isso nao tem "return"
      Serial.print("Status Botao API: ");
      payloadGlobal = payload;
      Serial.println(payloadGlobal);
    }

    http.end();   //Close connection
  }


}

//==================================================================================================== Pega hora e minuto do RTC
void ConsultaHoraAtualRTC() {

  RtcDateTime currentTime = rtcObject.GetDateTime();    //get the time from the RTC
  horas = currentTime.Hour();  //Variavel global por isso nao tem "return"
  minutos = currentTime.Minute(); //Variavel global por isso nao tem "return"
  Serial.print("Horario:");
  Serial.print(horas);
  Serial.print(":");
  Serial.println(minutos);
}

// ============================================================================ Consulta o tempo que o a solenoide ficara acionada
void ConsultaProgramacaoServidor() {

  String request_string = "http://172.16.231.2/smartcampus/jdvertical/c/index.php";
  String jsonReturn;
  if (client.connect("172.16.231.2", 80)) {
    http.begin(request_string);
    int httpCode = http.GET();

    if (httpCode < 0) {
      Serial.println("request error - " + httpCode);
      return ;
    }
    if (httpCode != HTTP_CODE_OK) {
      Serial.println("Code Error " + httpCode);
      return;
    }
    jsonReturn = http.getString(); //Retorna o json
    Serial.println(jsonReturn);
    DynamicJsonDocument doc(1024);
    deserializeJson(doc, jsonReturn);
    http.end();
    int time0 = (int)doc["time0"];
    int time1 = (int)doc["time1"];
    if (EEPROM.read(0) != time0) {
      EEPROM.write(0, time0);
      EEPROM.commit();
      Serial.println("endereco 0 atualizado");
    }
    if (EEPROM.read(1) != time1) {
      EEPROM.write(1, time1);
      EEPROM.commit();
      Serial.println("endereco 1 atualizado");
    }

    Serial.print("Time0: ");
    Serial.println(time0);
    Serial.print("Time1: ");
    Serial.println(time1);
    delay(1000);
  }
}

//==================================================================================================== Envia status da valvula para o thingspeak
void EnviaStatusValvulaAPI() {

  if (WiFi.status() == WL_CONNECTED) { //Check WiFi connection status
    
    HTTPClient http;  //Declare an object of class HTTPClient
  
    Serial.println("");
    Serial.println("Enviando status para o Thingspeak");
    
    String request_string;
    if (client.connect("api.thingspeak.com", 80)) {
  
      if (v1 == 1) {
        request_string = "http://api.thingspeak.com/update?api_key=KWW7K1HHDUZDWUIR&field1=1";
      } else if (v1 == 0) {
        request_string = "http://api.thingspeak.com/update?api_key=KWW7K1HHDUZDWUIR&field1=0";
      }
  
      http.begin(request_string);
  
      Serial.println(http.GET());
      
      digitalWrite(ledEnvio, HIGH); //
      delay (1000);                // Acionamento do led de indentificação de envio
      digitalWrite(ledEnvio, LOW); //
  
      http.GET();
      http.end();
    }
  
  }

}

void atualizaHora()
{
  //RtcDateTime currentTime = RtcDateTime(19, 07, 18, 13, 20, 10);

  String request_string = "http://172.16.231.2/smartcampus/jdvertical/currenttime.php";
  String jsonReturn;
  if (client.connect("172.16.231.2", 80)) {
    http.begin(request_string);
    int httpCode = http.GET();

    if (httpCode < 0) {
      Serial.println("request error - " + httpCode);
      return ;
    }
    if (httpCode != HTTP_CODE_OK) {
      Serial.println("Code Error " + httpCode);
      return;
    }
    jsonReturn = http.getString(); //Retorna o json
    Serial.println(jsonReturn);
    DynamicJsonDocument doc(1024);
    deserializeJson(doc, jsonReturn);
    http.end();
    int dia = (int)doc["dia"];
    int mes = (int)doc["mes"];
    int ano = (int)doc["ano"];
    int hora = (int)doc["hora"];
    int minuto = (int)doc["minuto"];
    int segundo = (int)doc["segundo"];
    //RtcDateTime currentTime = RtcDateTime(19, 07, 18, 13, 20, 10);
    RtcDateTime currentTime = RtcDateTime(ano, mes, dia, hora, minuto, segundo);
    rtcObject.SetDateTime(currentTime);
    Serial.print("Data Servidor: ");
    Serial.print(dia);
    Serial.print("/");
    Serial.print(mes);
    Serial.print("/");
    Serial.println(ano);
    Serial.print(" Hora Servidor: ");
    Serial.print(hora);
    Serial.print(":");
    Serial.print(minuto);
    Serial.print(":");
    Serial.print(segundo);
    delay(1000);
  }

}
