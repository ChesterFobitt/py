import os, pymongo, dateutil.parser
from bson import SON
import pandas as pd
import numpy as np

__uri__ = 'mongodb://localhost:27017/'

def generateXLSX(reglic, an, lids, tests):
	#dir = os.getcwd()  # используем os.getcwd() только для запуска из консоли
	dir = './server/app/controllers' # не используем os.getcwd() т.к. указывает на место вызова для child_process
	writer = pd.ExcelWriter(dir + '/output/report.xlsx')
	
	if not os.path.exists(dir + '/output'):
		os.makedirs(dir + '/output')

	titles = [
		'Дата создания',
		'Email',
		'Тип лицензии',
		'Продукт',
		'Сумма оплаты',
		'Дата оплаты',
		'Название лицензии',
		'ID',
		'Слушатель вебинаров',
		'Каналы',
		'Начало доступа',
		'Конец доступа',
		'Дата регистрации',
		'Сумма оплат',
		'Имя',
		'Телефон',
		'Компания',
		'Отрасль',
		'Last Click (source)',
		'Last Click (channel)',
		'Last Click (campaign)',
		'Multi-Channel Funnels'
	]
	an_titles = [
		'Дата оплаты',
		'Количество кликов зарегистрировавщихся',
		'Сумма расходов',
		'Количество кликов не зарегистрировавшихся',
		'Строка UTM',
		'Compaign UTM',
		'Medium UTM',
		'Source UTM',
		'Продукт'
	]
	lids_titles = [
		'Дата регистрации',
		'Продукт',
		'Medium UTM',
		'Source UTM',
		'Campaign UTM',
		'Сумма оплат',
		'Сумма расходов',
		'Число регистраций',
		'Число регистраций с оплатами'
	]
	reglic_headers = [
		'create_date','email','type','product','pay_summ','pay_date_time',
		'pay_name','license_id','is_webinar_user','channels','starting_date',
		'expiry_date','registration_date','pay_total','name','phone','company',
		'sector','last_click_source','last_click_channel','last_click_campaign','multi_channeel'
	]
	reglic = reglic[reglic_headers]
	an = an[['date', 'clicks_registered', 'total_registered', 'clicks_unregistered', 'source_string', 'source_compaign', 'source_medium', 'source_source', 'product']]
	lids = lids[['CD', 'product', 'last_click_channel', 'last_click_source', 'last_click_campaign', 'pay_summ', 'cf_trs', 'lid_count', 'lid_pay_count']]
	tests = tests[reglic_headers]
    
	reglic.to_excel(writer, sheet_name='Рег+Лиц', header=titles, index=False )
	an.to_excel(writer, sheet_name='Аналитика', header=an_titles, index=False )
	lids.to_excel(writer, sheet_name='Лиды', header=lids_titles, index=False )
	tests.to_excel(writer, sheet_name='Тестовые', header=titles, index=False)

	writer.save()


def endDataSet(registration, licenses, analitics):
	 
	reg, lic, an = pd.DataFrame(registration), pd.DataFrame(licenses), pd.DataFrame(analitics)
	
  # merge registration and licenses
	reglic = pd.merge(lic, reg, on=['email', 'product'], how="inner")
	
  # filter test users
	reglic_test = reglic[(reglic["email"].str.find('@mailin') != -1) | (reglic["email"].str.find('@pravo.ru') != -1) | (reglic["email"].str.find('@parcsis') != -1) | (reglic["email"].str.find('test') != -1) | (reglic["email"].str.find('pravonatest') != -1)]
	reglic = reglic[(reglic["email"].str.find('@mailin') == -1) & (reglic["email"].str.find('@pravo.ru') == -1) & (reglic["email"].str.find('@parcsis') == -1) & (reglic["email"].str.find('test') == -1) & (reglic["email"].str.find('pravonatest') == -1)]
    
	# delete rows with empty dates
	reglic.dropna(axis=0, inplace=True, subset=['create_date'])
    
	# generate dates
	reglic['CD'] = reglic.create_date.apply(lambda d: d.strftime('%Y-%m-%d'))
	an['CD'] = an.date.apply(lambda d: d.strftime('%Y-%m-%d'))
	reglic.fillna('', inplace=True)
    
	notemptysource = reglic.query('last_click_channel != "" and last_click_source != "" and last_click_campaign != ""')
	lidogen = notemptysource[['CD','product','last_click_channel','last_click_source','last_click_campaign', 'pay_summ']].copy()

	lidogen.loc[:,('lid_count')] = lidogen.duplicated(['CD', 'product','last_click_channel','last_click_source','last_click_campaign']).transform(np.size)
	lidogen.loc[:,('lid_pay_count')] = lidogen.query('pay_summ > 0').duplicated(['CD', 'product','last_click_channel','last_click_source','last_click_campaign']).transform(np.size)
	lidogen.lid_pay_count.fillna(0, inplace=True)
    
	lidogen['count_factor'] = 0
	lidogen['count_factor'] = lidogen.groupby(['CD', 'product', 'last_click_channel', 'last_click_source']).transform(np.size)
	lidogen['count_factor'] = [1/item * 1.125 for item in lidogen['count_factor']]
	lidogen['count_factor'] = [round(item,2) for item in lidogen['count_factor']]

	data_summ_an = an[['CD', 'product', 'source_medium', 'source_source', 'total_registered']].groupby(['CD', 'product', 'source_medium', 'source_source']).aggregate(sum)
	data_summ_an = data_summ_an.add_suffix('_summ').reset_index()

	lids = pd.merge(lidogen, data_summ_an, left_on=['CD', 'product','last_click_channel','last_click_source'], right_on=['CD', 'product', 'source_medium', 'source_source'], how="left")

	lids.fillna(0, inplace=True)
	lids.loc[:, ('cf_trs')] = 0

	lids.loc[:, ('cf_trs')] = lids[['count_factor', 'total_registered_summ']].prod(axis=1)
	lids.cf_trs = lids.cf_trs.round(2)
        
	generateXLSX(reglic, an, lids, reglic_test)
	
def utmParse(utm, outputArray):
	result = {}

	if (not utm):
		return ''

	utm = utm.replace('?', '')

	for part in utm.split('&'):
		item = part.split('=')
		result[item[0]] = item[1]

	output = result.get('utm_source', '') + '/' + result.get('utm_medium', '') + ' ' + result.get('utm_campaign', '')

	if (outputArray):
		return result
	else:
		return output


def mapUtm(item):
	return utmParse(item['utm'], False)


def userRegistration(collection):
	dataset = []
	id = ''

	cursor = collection.aggregate([		
		{
			'$unwind': '$products'
		},
		{
			'$match': {
				'products.project.registration_date': {
					'$gte': dateutil.parser.parse('2017-09-01T00:00:00.000Z')
				}
			}
		},
		{
			'$project': {
				'email': 1,
				'products': 1,
				'pay_total': 1,
				'contacts': 1,
				'__v': 1
			}
		},
		{
			'$sort': SON([('products.project.registration_date', 1)])
		}
	])
	
	for user in cursor:
		products = user['products']
		project = products['project']
		company = project.get('company', '')
		last_utm_source = ''
		last_utm_medium = ''
		last_utm_campaign = ''
		sector = ''
		productName = 'product_one' if (products['_id'] == id) else 'product_two'
		
		try:
			utm = map(mapUtm, project["utm_sources"])
			list_sources = ','.join(utm)
		except AttributeError:
			print('Utm is empty')

		if ('last_mark' in project):
			if ('utm' in project['last_mark']):
				lastUtm = utmParse(project['last_mark']['utm'], True)

				last_utm_source = lastUtm['utm_source'] if (type(lastUtm) == dict and 'utm_source' in lastUtm) else ''
				last_utm_medium = lastUtm['utm_medium'] if (type(lastUtm) == dict and 'utm_medium' in lastUtm) else ''
				last_utm_campaign = lastUtm['utm_campaign'] if (type(lastUtm) == dict and 'utm_campaign' in lastUtm) else ''

		if ('sector' in project):
			if ('name' in project['sector']):
				sector = project['sector']['name']

		dataset.append({
			'registration_date': project['registration_date'],
			'product': productName,
			'pay_total': project['pay_total'],
			'name': project['name'],
			'phone': project['phone'],
			'company': company,
			'email': user['email'],
			'sector': sector,
			'last_click_source': last_utm_source,
			'last_click_channel': last_utm_medium,
			'last_click_campaign': last_utm_campaign,
			'multi_channeel': list_sources
		})

	else:
		return dataset

def userLicenses(collection):
	dataset = []
	id = ''

	cursor = collection.find(
		{
			'create_date': {
				'$gte': dateutil.parser.parse('2017-09-01T00:00:00.000Z')
			}
		}
	).sort([
	  ('create_date', pymongo.ASCENDING)
	])

	for contract in cursor:
		productName = 'product_one' if (contract['project'] == id) else 'product_two'
		is_webinar_user = 'Да' if (contract.get('is_webinar_user', '')) else 'Нет'

		if(isinstance(contract.get('channels', ''), list)):
			if (len(contract['channels']) > 1):
				channels = ','.join(contract['channels']).replace('"', '').replace('[', '').replace(']', '')
			elif (len(contract['channels']) == 1):

				channels = contract['channels'][0]
			else:
				channels = ''
				
		try:
			dataset.append({
				'create_date': contract.get('create_date', ''),
				'email': contract.get('email', ''),
				'type': contract.get('license_type', ''),
				'product': productName,
				'pay_summ': contract.get('pay_summ', ''),
				'pay_date_time': contract.get('pay_date_time', ''),
				'pay_name': contract.get('name', ''),
				'license_id': contract.get('license_id', ''),
				'is_webinar_user': is_webinar_user,
				'channels': channels,
				'starting_date': contract.get('starting_date', ''),
				'expiry_date': contract.get('expiry_date', '')
			})
		except KeyError:
			print(contract)

	else:
		return dataset

def userAnalitics(collection):
	dataset = []

	cursor = collection.find({
		'date': {
			'$gte': dateutil.parser.parse('2017-09-01T00:00:00.000Z')
		}
	}).sort([
	  ('date', pymongo.ASCENDING)
	])

	for cost in cursor:
		utm_sources = utmParse(cost['sourceString'], True)

		try:
			dataset.append({
				'date': cost.get('date', ''),
				'clicks_registered': cost.get('clicks_registered', ''),
				'total_registered': cost.get('total_registered', '') + cost.get('total_unregistered', ''),
				'clicks_unregistered': cost.get('clicks_unregistered', ''),
				'source_string': cost.get('sourceString', ''),
				'source_compaign': utm_sources.get('utm_campaign', ''),
				'source_medium': utm_sources.get('utm_medium', ''),
				'source_source': utm_sources.get('utm_source', ''),
				'product': cost.get('product', '')
			})
		except KeyError:
			print(cost)

	else:
		return dataset

def main():
	try:
		client = pymongo.MongoClient(__uri__)
		database = client.billing
		collection_reg = database['projectusers']
		collection_licenses = database['projectlicenses']
		collection_analitics = database['analitics']
		registration = userRegistration(collection_reg)
		licenses = userLicenses(collection_licenses)
		analitics = userAnalitics(collection_analitics)

		return endDataSet(registration, licenses, analitics)

	except:
		print('error')
		raise

	finally:
		print('done')
		client.close()

if __name__ == "__main__":
	main()
	
