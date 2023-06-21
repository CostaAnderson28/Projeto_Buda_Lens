import paho.mqtt.client as mqtt 
import pandas as pd
import time

prov_msg_luz = str
prov_msg_Temp = str
prov_msg_CO2= str

dados = {'data/hora':[],'luminosidade':[]} #,'temperatura ':[],'CO2':[]} 
# Cria um Dicionário onde serão inseridas as informações 

def tratamento(a): 
    # função onde os dados são separandos e inserindos no dicionário
    
    global dados 
    if a[0] == 'L':
        info = a[5:8]
        hora , parcial_data = a[21:29], a[39:48]
        data = parcial_data[7:9]+"/"+parcial_data[5:6]+"/"+parcial_data[:4]
        data_hora = data + '-' + hora
        dados['data/hora'].append(data_hora)
        dados['luminosidade'].append(info)

    #if a[0] == 'T':

     #   dados['temperatura'].append(a[14:16] + " ºC")

    #if a[0] == 'C':

     #   dados['CO2'].append(a[5:11])
    return dados

def on_messege(clinet, userdata, message): 
    # Função de recebimento das mensagens do broker onde a função "tratamento" está inserida
    
    if str(message.payload.decode('utf-8'))[0] == 'L':
        global prov_msg_luz 
        prov_msg_luz = str(message.payload.decode('utf-8'))
        print('Menssagem Recebida', str(message.payload.decode('utf-8')))
        return tratamento(prov_msg_luz)
    
    if str(message.payload.decode('utf-8'))[0] == 'T':
        global prov_msg_Temp 
        prov_msg_Temp = str(message.payload.decode('utf-8'))
        print('Menssagem Recebida', str(message.payload.decode('utf-8')))
        return tratamento(prov_msg_Temp)
    
    if str(message.payload.decode('utf-8'))[0] == 'C':
        global prov_msg_CO2 
        prov_msg_CO2 = str(message.payload.decode('utf-8'))
        print('Menssagem Recebida', str(message.payload.decode('utf-8')))
        return tratamento(prov_msg_CO2)

mqttBroker = 'test.mosquitto.org'
client = mqtt.Client('API_test')
client.connect(mqttBroker)
# Conexão com o broker

client.loop_start()
client.subscribe('Labnet/Luz')
client.subscribe('Labnet/CO2')
client.subscribe('Labnet/TEMP')
client.on_message = on_messege 
time.sleep(10)
client.loop_stop()
# Configuração de recebimneto de mensgem do topico "Labnet/Luz"

dados_df = pd.DataFrame(dados)
# Criação do DataFrame a partir do dicionario "Dados"

print(dados_df)

