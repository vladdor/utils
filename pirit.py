import asyncio
import asyncssh
import asyncpg
import logging

asyncssh.set_log_level(40)
logging.basicConfig(
	filename='pirit_info.log',
	level=logging.INFO, 
	format='%(message)s',
	datefmt='%Y-%m-%d %H:%M:%S'
	)


def message(command, data):
	"""
		Формирует пакет для ККМ
	@params:
		command   - Команда, 2 символа (str)
		data	  - Данные, разденены запятой (str)
	"""
	data = [x for x in data]
	for n, i in enumerate(data):
		if i == ',':
			data[n] = 28
			continue
		data[n] = ord(i)
	frame = bytearray()
	frame += 'PIRI'.encode()			# пароль
	frame.append(0x21)				  # id пакета
	frame.append(ord(command[0]))	   # первый байт команды
	frame.append(ord(command[1]))	   # второй байт команды
	frame.extend(data)				  # данные
	frame.append(0x03)				  # конец пакета
	crc_calc = None	
	for i in frame:
		if crc_calc is None:
			crc_calc = i
			continue
		crc_calc ^= i
	crc = [ord(x) for x in str(hex(crc_calc)).split('x')[1]] # контрольная сумма
	if len(crc) < 2:
		crc.insert(0, 48)
	result = bytearray()
	result.append(0x02)
	result.extend(frame)
	result.extend(crc)
	return result


def get_data(response):
	response = response.decode('cp866')
	if len(response) < 9:
		return None
	id = response[1]
	if id != '!':
		return None
	error = response[4:6]
	if error != '00':
		return None
	data = ''.join(response[6:-3])
	result = data.replace('\x1c',',')
	if len(result) > 1:
		result = result[:-1]
	return result

	
async def run_client(host):
	try:
		async with asyncssh.connect(host, known_hosts=None,  username='', password='') as conn:
			await conn.run('sudo su -c "socat tcp-l:12345 /dev/usbPIRIT0 > /dev/null 2>&1 &"')
	except Exception as exc:
		logging.error('{0};{1}'.format(host, exc))
	
	try:
		reader, writer = await asyncio.open_connection(host, 12345)
		#('00','')			#status
		#('02','2')			#firmware
		#('11','30,2')	#address
		
		commands = [('02','2')]
		response = []
		for command in commands:
			writer.write(message(command[0], command[1]))
			info = await reader.read(100)
			info = get_data(info)
			response.append(info)
		
		result = ';'.join(response)
		logging.info('{0};{1}'.format(host,result))
	except Exception as err:
		logging.error('{0};{1}'.format(host, err))
		return


async def run_multiple_clients():
	conn = await asyncpg.connect('postgresql://postgres:postgres@ip/db')
	shift = await conn.fetch('''
		select cash_ip from cash_cash where status='ACTIVE' and cash_ip is not NULL
		''')
	hosts = [x[0] for x in shift]
	tasks = (run_client(host) for host in hosts)
	await asyncio.gather(*tasks)

	
loop = asyncio.get_event_loop()
loop.run_until_complete(run_multiple_clients())
