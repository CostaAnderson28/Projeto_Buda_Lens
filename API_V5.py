import paho.mqtt.client as mqtt 
import time

prov_msg_luz = ""
prov_msg_Temp = ""
prov_msg_CO2 = ""

final_data = ""
final_CO2 = ""
final_luz = ""
final_Temp = "" 
# Cria um Dicionário onde serão inseridas as informações 

def tratamento(a): 
    # função onde os dados são separandos e inserindos no dicionário
    
    global final_CO2
    global final_luz
    global final_Temp
    global final_data

    if a[0] == 'L':
        luz_mensagem = a
        luz_mensagem = luz_mensagem.split(" ")
        luz = int(luz_mensagem[1])
        final_luz = luz

    if a[0] == 'T':

        temp_mensagem = a
        temp_mensagem = temp_mensagem.split(" ")
        temp = int(temp_mensagem[1])
        hora_temp = temp_mensagem[5].replace("]-","")
        data_temp = temp_mensagem[7].replace("]","")
        data_temp = data_temp.split("-")
        ano = data_temp[0]
        mes = data_temp[1]
        dia = data_temp[2]

        if len(dia)<2:
            dia = "0" + dia

        if len(mes)<2:
            mes = "0" + mes

        data_temp = ano + "-" + mes + "-" + dia
        final_data = data_temp + " " + hora_temp
        final_Temp = temp

    if a[0] == 'C':

        co2_mensagem =  a
        co2_mensagem = co2_mensagem.split(" ")
        co2 = int(co2_mensagem[1])
        final_CO2 = co2
    


def on_messege(client, userdata, message): 
    # Função de recebimento das mensagens do broker onde a função "tratamento" está inserida
    if str(message.payload.decode('utf-8'))[0] == 'L':
        global prov_msg_luz 
        prov_msg_luz = str(message.payload.decode('utf-8'))
        return tratamento(prov_msg_luz)
    
    if str(message.payload.decode('utf-8'))[0] == 'T':
        global prov_msg_Temp 
        prov_msg_Temp = str(message.payload.decode('utf-8'))
        return tratamento(prov_msg_Temp)
    
    if str(message.payload.decode('utf-8'))[0] == 'C':
        global prov_msg_CO2 
        prov_msg_CO2 = str(message.payload.decode('utf-8'))
        return tratamento(prov_msg_CO2)

mqttBroker = 'test.mosquitto.org'
client = mqtt.Client('API_test')
client.connect(mqttBroker)

# Conexão com o broker

while(True):
    client.loop_start()
    client.subscribe('Labnet/Luz')
    client.subscribe('Labnet/CO2')
    client.subscribe('labnet/TEMP')
    client.on_message = on_messege
    time.sleep(2)
    client.loop_stop()
    # ('YYYY-MM-DD HH:MM:SS', 43, 35, 67)
    time.sleep(10)

# Configuração de recebimneto de mensgem do topico "Labnet/Luz"
