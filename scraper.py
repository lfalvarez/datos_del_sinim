
import requests
from bs4 import BeautifulSoup
import scraperwiki
import unicodecsv
import wikipedia

wikipedia.set_lang('es')

ids =["11201","05602","13502","08314","03302","01107","10202","04103","09201","02101","08302","08202","15101","13402","08402","05402","12201","08303","08203","02201","10102","03102","05502","13403","05302","15102","01402","04202","09102","05603","05102","10201","05702","07201","13102","13103","03201","10401","07202","08103","11401","08401","08406","06303","09121","10203","06302","11202","08403","10103","11301","06102","08404","11101","08405","06103","07402","01403","13301","09202","06104","04302","08101","13104","05103","07102","08204","03101","04102","08102","14102","09103","09203","13503","10204","08205","09104","07103","07301","10205","03202","06105","13105","08407","13602","05604","05605","07104","09204","13106","08104","09105","03303","10104","10105","10402","14202","09106","15202","09107","06106","11203","05503","07302","10403","08112","08105","01404","03304","13107","04201","13108","01101","08409","13603","05201","05104","13109","05504","06202","13110","14203","11102","13111","12102","04104","08304","05401","13302","14103","13112","13113","06107","13114","04101","14201","09108","08201","07303","05802","07401","06203","05703","10107","13115","13116","06304","09109","07403","09205","13117","05301","14104","08206","10106","08301","09206","04203","08106","09207","06108","13118","13119","06109","02302","13504","06204","14106","07105","10108","02102","09110","13501","14105","07304","04303","06110","08305","08306","06305","12401","06205","08307","08408","05506","09111","11302","06111","02202","05803","10301","04301","13604","09112","04105","14107","13404","10404","06306","14108","05704","05403","06206","07404","13605","13122","13121","07106","07203","08410","07107","08107","06307","09113","05404","06112","01405","06113","06201","08411","13202","09114","06308","08412","12301","01401","12302","13123","05105","09115","13124","13201","10101","10302","10109","06309","04304","12101","10206","09208","10303","05705","15201","10304","10207","10208","10209","08308","13125","08309","08413","05501","05801","10210","06114","13126","05107","08414","06101","07305","13127","09209","13128","06115","06116","07405","05303","08415","14204","07108","04305","11402","07306","10305","12103","09116","07307","04204","05601","13401","08416","07109","05304","08417","05701","06301","12104","08418","07406","13129","13203","10306","13130","08419","10307","13505","02203","08108","07110","13131","08310","08311","06310","08109","05706","13101","05606","06117","02103","13601","07101","08110","02104","09101","07308","09117","03103","13303","12303","08207","02301","09118","08111","12402","11303","09210","08420","08312","13120","14101","03301","05101","05109","07309","09211","04106","09119","07407","05804","09120","13132","07408","08313","08421","05405"]
#ids = ["11201", "05602"]


def process_municipality(id_sinim, id=1, concejal_counter=1):
    page = requests.post('http://datos.sinim.gov.cl/ficha_comunal.php', data = {"municipio": id_sinim})
    tree = BeautifulSoup(page.content, 'lxml')
    alcalde= tree.select('.nombre_alcalde > h3')[0].text
    partido_pacto_alcalde = tree.select('.nombre_alcalde > h4')[1].text.split(' - ')
    partido_alcalde = partido_pacto_alcalde[0]
    pacto_alcalde = partido_pacto_alcalde[1]
    web_muni = tree.select('.info_municipio > dl > dd')[6].text
    nombre_muni = tree.select('.tit_comuna')[0].text
    scraperwiki.sqlite.save(unique_keys=['id'], data={"id": concejal_counter,
                                                      "nombre": alcalde,
                                                      "id_muni": id,
                                                      "partido": partido_alcalde,
                                                      "posicion": "alcalde",
                                                      "pacto": pacto_alcalde}, table_name="personas")
    concejal_counter += 1
    scraperwiki.sqlite.save(unique_keys=['id'], data={"id": id,
                                                      "id_sinim":id_sinim,
                                                      "muni": nombre_muni,
                                                      "web_muni": web_muni})

    concejales = tree.select('#tab-autoridades .file')

    for concejal in concejales:
        nombrec = concejal.select('.col_nom')[0].text
        partidoc = concejal.select('.col_partido')[0].text
        pactoc = concejal.select('.col_pacto')[0].text
        scraperwiki.sqlite.save(unique_keys=['id'], data={"id": concejal_counter,
                                                          "nombre": nombrec,
                                                          "id_muni": id,
                                                          "partido": partidoc,
                                                          "posicion": "concejal",
                                                          "pacto": pactoc}, table_name="personas")
        concejal_counter += 1
    id += 1
    return concejal_counter, id


def process_casen():
    f = open('casen_2014.csv', 'r')
    r = unicodecsv.reader(f, encoding='utf-8')
    comunas_names = r.next()
    datos_comuna = {}
    for i in range(3, len(comunas_names)):
        comuna = comunas_names[i].upper()
        result = scraperwiki.sqlite.select('id from data where muni="%s"' % (comuna, ))
        id_ = result[0].get('id')
        datos_comuna[i] = {'comuna_id': id_, 'comuna_name': comuna, 'dato': []}
    dato_counter = 0
    for datos in r:
        dato_counter += 1
        for j in range(3, len(comunas_names)):
            datos_comuna[j]['dato'].append({
                'id': dato_counter,
                'dato_name': datos[1].strip(),
                'value': datos[j]
            })
    dato_counter = 0
    for a in datos_comuna:
        for b in datos_comuna[a]['dato']:
            final = {'id': dato_counter,
                     'id_muni': datos_comuna[a]['comuna_id'],
                     'dato_name': b['dato_name'],
                     'value': b['value']
            }
            dato_counter += 1
            scraperwiki.sqlite.save(unique_keys=['id'], data=final, table_name='datos_comuna')



def process_wiki():
    comunas = wikipedia.page("Anexo:Comunas_de_Chile")
    tree = BeautifulSoup(comunas.html(), 'lxml')
    trs = tree.select('table > tr')[1:]
    counter = 0
    for tr in trs:
        tds = tr.select('td')
        comuna_code = tds[0].text

        link = tds[1].select('a')[0]
        url_completa = 'http://es.wikipedia.com/'+link.get('href')
        result = scraperwiki.sqlite.select('id from data where id_sinim="%s"' % (comuna_code, ))
        if not result:
           print comuna_code, link.text, url_completa
        else:
            id_ = result[0].get('id')
            print "<-----------------------------------------"
            try:
                title = link.get('title')
                page = wikipedia.page(title)
                scraperwiki.sqlite.save(unique_keys=['id'], data={"id": counter,
                                                                  "wiki_link": link.get('href'),
                                                                  "id_muni": id_,
                                                                  "url_completa": url_completa,
                                                                  "summary": page.summary
                                                                  }, table_name="extra_info_comuna")
            except:
                print '*******************'
                print 'No funciona con '+link.text
                print '*******************'
            counter += 1



counter = 1
concejal_counter = 1
for id_sinim in ids:
    concejal_counter, counter = process_municipality(id_sinim, id=counter, concejal_counter=concejal_counter)
process_casen()

process_wiki()
