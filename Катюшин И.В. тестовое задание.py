#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd


# In[2]:


# начинаю сбор датасета с основных данных о населении РФ и Белгородской области
# с разделением на городское и сельское население
# данные получены с официального сайта Федеральной сулжбы государственной статистики
data = pd.read_excel(r"C:\Users\ikaty\OneDrive\Документы\данные\naselenie.xlsx", skiprows=1, usecols="A:H")


# In[3]:


display(data.head(10))


# In[4]:


# очистка и фильтрация полученных данных 
data = data.rename(columns = {'Unnamed: 0' : 'year',
                              'Российская Федерация все население' : 'RU ALL',
                              'Российская Федерация городское население':'RU CITY',
                              'Российская Федерация сельское население':'RU RURAL',
                              'Белгородская область все население':'BELG ALL',
                              'Белгородская область городское население':'BELG CITY',
                              'Белгородская область сельское население':'BELG RURAL',})


# In[5]:


#удаление лишних строк и столбцов
data = data.drop(data.index[0])
data = data.drop(columns=['Unnamed: 1'],axis=1)


# In[6]:


# проверка результата, следует превести таблицу к соотвествующему типу данных
data.head()


# In[7]:


data.info()


# In[8]:


#простая функция для преобразования данных по датам для последующего их перевода в цифровой тип
def year_int(year):
    new_val_year = year.replace(' г.','')
    return new_val_year


# In[9]:


#применения функции, перевод всех данных в целочисленный формат
data['year']=data['year'].apply(year_int)
data = data.astype(int)


# In[10]:


#проверяем полученный ДатаФрейм, вышло как и запланировано, сохраню его в отдельном файле для безопасности и последующего объединения


# In[11]:


#выбрал формат CSV так как с ним мне удобнее работать и он менее требователен к занимаемому месту на диске чем xlsx
data.to_csv (r"C:\Users\ikaty\OneDrive\Документы\данные\naselenie.csv",index = False,sep =';')


# In[12]:


#проверяю, что запись прошла успешно и создаю переменную для будущего объединения данных
naselenie_df = pd.read_csv(r"C:\Users\ikaty\OneDrive\Документы\данные\naselenie.csv",sep=';')
naselenie_df.head()


# In[13]:


#таблице явно не хватает процетного соотношения городского и сельского населения с общим количеством людей,
#поэтому произвожу необходимые расчеты и распределяю полученные данные в нужных местах рабочего ДатаФрейма 
percent_ru_city = round(naselenie_df['RU CITY']/naselenie_df['RU ALL']*100, 2)
percent_ru_rural = round(naselenie_df['RU RURAL']/naselenie_df['RU ALL']*100, 2)
naselenie_df.insert(3, 'PER_RU_CITY', percent_ru_city, allow_duplicates = False)
naselenie_df.insert(5, 'PER_RU_RURAL', percent_ru_rural, allow_duplicates = False)
percent_belg_city = round(naselenie_df['BELG CITY']/naselenie_df['BELG ALL']*100, 2)
percent_belg_rural = round(naselenie_df['BELG RURAL']/naselenie_df['BELG ALL']*100, 2)
naselenie_df.insert(8, 'PER_BELG_CITY', percent_belg_city, allow_duplicates = False)
naselenie_df.insert(10, 'PER_BELG_RURAL', percent_belg_rural, allow_duplicates = False)


# In[14]:


naselenie_df


# In[15]:


#в данном случае я не стал создавать новую переменную для DF так как я уже сохранил первую часть и обрабатываемые данные
#не требуют создания новых переменных, поэтому считаю, что одной будет достаточно
data = pd.read_excel(r"C:\Users\ikaty\OneDrive\Документы\данные\prirost.xlsx", skiprows=2, usecols="A:H")


# In[16]:


#второй файл соедржит информацию о приросте населения, он технически аналогичен первому
#поэтому делаю те же действия, что и с первым файлом 
data.head()


# In[17]:


data = data.rename(columns = {'Оба пола, 23230000100010200001 Естественный прирост населения за год' : 'year',
                              'Unnamed: 2' : 'RU ALL prirost',
                              'Unnamed: 3':'RU CITY prirost',
                              'Unnamed: 4':'RU RURAL prirost',
                              'Unnamed: 5':'BELG ALL prirost',
                              'Unnamed: 6':'BELG CITY prirost',
                              'Unnamed: 7':'BELG RURAL prirost',})


# In[18]:


data = data.drop(columns=['12'],axis=1)


# In[19]:


data.head()


# In[20]:


data['year']=data['year'].apply(year_int)
data = data.astype(int)


# In[21]:


data.info()


# In[22]:


#так как второй файл отфильтрован и очищен, теперь можно приступать к обьединению данных и создать Датасет
merged = naselenie_df.merge(
    data,
    on='year',
    how='left'
)
display(merged.head())
merged.to_csv(r"C:\Users\ikaty\OneDrive\Документы\данные\ru_belg_dataset.csv", index=False, sep=';')


# In[23]:


#обьединение прошло успешно, создаем для датасета переменную
bigdata = pd.read_csv(r"C:\Users\ikaty\OneDrive\Документы\данные\ru_belg_dataset.csv",sep=";")


# In[24]:


#третий файл содержит данные о рождаемости, для более точных данных
#я принял решения взять данных без учета мертворожденных
data = pd.read_excel(r"C:\Users\ikaty\OneDrive\Документы\данные\rojdenie.xlsx", skiprows=2, usecols="A:H")


# In[25]:


data.head()


# In[26]:


data = data.rename(columns = {'Оба пола, 23210000100020200001 Число родившихся (без мертворожденных) за год' : 'year',
                              'Unnamed: 2' : 'RU ALL rojdaemost',
                              'Unnamed: 3':'RU CITY rojdaemost',
                              'Unnamed: 4':'RU RURAL rojdaemost',
                              'Unnamed: 5':'BELG ALL rojdaemost',
                              'Unnamed: 6':'BELG CITY rojdaemost',
                              'Unnamed: 7':'BELG RURAL rojdaemost',})
data = data.drop(columns=['12'],axis=1)
data['year']=data['year'].apply(year_int)
data = data.astype(int)
data.info()


# In[27]:


data.head()


# In[28]:


#объединяю третий файл с Датасетом
merged = bigdata.merge(
    data,
    on='year',
    how='left'
)
display(merged.head())
merged.to_csv(r"C:\Users\ikaty\OneDrive\Документы\данные\ru_belg_dataset.csv", index=False, sep=';')


# In[29]:


#четвертый файл содержит информацию о смертности, в том числе мертворожденные
data = pd.read_excel(r"C:\Users\ikaty\OneDrive\Документы\данные\smertnost.xlsx", skiprows=2, usecols="A:H")


# In[30]:


data.head()


# In[31]:


data = data.rename(columns = {'Оба пола, 23220000100020200002 Число умерших за год' : 'year',
                              'Unnamed: 2' : 'RU ALL smertnost',
                              'Unnamed: 3':'RU CITY smertnost',
                              'Unnamed: 4':'RU RURAL smertnost',
                              'Unnamed: 5':'BELG ALL smertnost',
                              'Unnamed: 6':'BELG CITY smertnost',
                              'Unnamed: 7':'BELG RURAL smertnost',})
data = data.drop(columns=['12'],axis=1)
data['year']=data['year'].apply(year_int)
data = data.astype(int)
data.info()


# In[32]:


data.head()


# In[33]:


bigdata = pd.read_csv(r"C:\Users\ikaty\OneDrive\Документы\данные\ru_belg_dataset.csv",sep=";")


# In[34]:


#объединяю четвертый файл с ранее созданным Датасетом
merged = bigdata.merge(
    data,
    on='year',
    how='left'
)
display(merged.head())
merged.to_csv(r"C:\Users\ikaty\OneDrive\Документы\данные\ru_belg_dataset.csv", index=False, sep=';')


# In[35]:


#пятый, последний файл в моем отчете содержит информацию о миграции, внутри странны, между странами состоящми в СНГ,
#а также с зарубежными странами не входящими в число стран-участниц СНГ
data = pd.read_excel(r"C:\Users\ikaty\OneDrive\Документы\данные\migracia.xlsx", skiprows=2, usecols="A:J")


# In[36]:


data.head()


# In[37]:


data = data.rename(columns = {'23320000100030200007 Миграционный прирост (убыль) населения, абсолютные данные' : 'year',
                              'Unnamed: 2' : 'RU mejregion',
                              'Unnamed: 3':'BELG mejregion',
                              'Unnamed: 4':'RU mejdunarod',
                              'Unnamed: 5':'BELG mejdunarod',
                              'Unnamed: 6':'RU zarubej',
                              'Unnamed: 7':'BELG zarubej',
                              'Unnamed: 8':'RU obshee',
                              'Unnamed: 9':'BELG obshee'})
data = data.drop(columns=['Unnamed: 1'],axis=1)
data['year']=data['year'].apply(year_int)
data = data.astype(int)
data.info()


# In[38]:


bigdata = pd.read_csv(r"C:\Users\ikaty\OneDrive\Документы\данные\ru_belg_dataset.csv",sep=";")


# In[39]:


#обьединяю последний файл и создаю окончательный Датасет, которыя я буду использовать для создания графики
merged = bigdata.merge(
    data,
    on='year',
    how='left'
)
display(merged.head())
merged.to_csv(r"C:\Users\ikaty\OneDrive\Документы\данные\ru_belg_dataset.csv", index=False, sep=';')


# In[40]:


bigdata = pd.read_csv(r"C:\Users\ikaty\OneDrive\Документы\данные\ru_belg_dataset.csv",sep=";")


# In[41]:


#проверяю конечный файл, в целом все прошло хорошо, все данные сошлись правильно по дате
#я принял решение использовать графики из библиотеки plotly и matplotlib, так как данные ограничены, что мешает показать наглядно 
#некоторую информацию через стандартные инструменды графики в библиотеке Pandas
bigdata.info()


# In[42]:


import plotly
import plotly.express as px
import matplotlib.pyplot as plt
get_ipython().run_line_magic('matplotlib', 'inline')


# In[43]:


#график населения России показывает зависимоть общего числа населения от городского, виден некая "ямка" в период с 2000 по 2015
#при этом видно постепенно преобладание городского населения над сельским, в больших городах население растет в то время сельское падает
line_data = bigdata.groupby('year', as_index=False).sum()
fig = px.line(
    data_frame=line_data, #DataFrame
    x='year', #ось абсцисс
    y=['RU ALL'], #ось ординат
    height=600, #высота
    width=1000, #ширина
    title='Население РФ' #заголовок
)
fig.show()
fig.write_html("C:/Users/ikaty/OneDrive/Документы/данные/Население РФ.html")


# In[44]:


#рисунок графика общего количества населлния Белгородской обл явно отличаетсяот рисунка графика населения по РФ
line_data = bigdata.groupby('year', as_index=False).sum()
fig = px.line(
    data_frame=line_data, 
    x='year', 
    y=['BELG ALL'], 
    height=600, 
    width=1000, 
    title='Население Белгородской обл общее' 
)
fig.show()
fig.write_html("C:/Users/ikaty/OneDrive/Документы/данные/Население Белгородской обл.html")


# In[45]:


#городское и селькогое население РФ в сравнении друг с другом
group_perc_ru = bigdata.groupby(['year'])[['PER_RU_CITY', 'PER_RU_RURAL']].last()
group_perc_ru.plot(
    kind='bar',
    grid=True,
    figsize=(15, 7)
);


# In[46]:


#городское и селькогое население Белгородской обл в сравнении друг с другом
group_perc_belg = bigdata.groupby(['year'])[['PER_BELG_CITY', 'PER_BELG_RURAL']].last()
group_perc_belg.plot(
    kind='bar',
    grid=True,
    figsize=(15, 7)
);


# In[47]:


#сравнение процентного соотношения населения РФ в 2020 году и в 1990 году
#визуализация главного графика
fig = plt.figure(figsize=(5, 5))
main_axes = fig.add_axes([0, 0, 1, 1])
main_axes.pie(group_perc_ru.loc[2020],
    labels=group_perc_ru.columns,
    autopct='%.1f%%',
    explode = [0.1, 0]
);
main_axes.set_title('2020', fontsize=16) 

#визуализация вспомогательного графика
insert_axes = fig.add_axes([1.2, 0.2, 0.7, 0.7])
insert_axes.pie( group_perc_ru.loc[1990],
    labels=group_perc_ru.columns,
    autopct='%.1f%%',
    explode = [0.1, 0],
 
);

insert_axes.set_title('1990', fontsize=16) 


# In[48]:


#сравнение процентного соотношения населения Белгородской обл в 2020 году и в 1990 году
#визуализация главного графика
fig = plt.figure(figsize=(5, 5))
main_axes = fig.add_axes([0, 0, 1, 1])
main_axes.pie(group_perc_belg.loc[2020],
    labels=group_perc_belg.columns,
    autopct='%.1f%%',
    explode = [0.1, 0]
);
main_axes.set_title('2020', fontsize=16) 

#визуализация вспомогательного графика
insert_axes = fig.add_axes([1.2, 0.2, 0.7, 0.7])
insert_axes.pie( group_perc_belg.loc[1990],
    labels=group_perc_belg.columns,
    autopct='%.1f%%',
    explode = [0.1, 0],
 
);

insert_axes.set_title('1990', fontsize=16) 


# In[49]:


#На данном графике видно, что с 1990 по 1993 год в России происходит падение рождаемости и рост смертности населения
#отрицательная картина наблюдается влоть до 1997 года, когда Правительством вносится выплата материнского капитала за рождение ребенка
#и с 2007 по 2016 годы наблюдается рост рождаемости в стране,а также падение смертности в связи с улучшениями благосостояния населения, улучшения медицинской сферы
#при этом видно что в 2020 году произошел резкий рост смертностии на фоне короновирусной инфекции COVID-19

line_data = bigdata.groupby('year', as_index=False).sum()
fig = px.line(
    data_frame=line_data, 
    x='year', 
    y=['RU ALL rojdaemost', 'RU ALL smertnost', 'RU ALL prirost'], 
    height=500, 
    width=1000, 
    title='Рождаемость, смертность, прирост населения Россия' 
)
fig.show()


# In[50]:


#данный график показывает схожую картину с предыдущим графиком, падение рождаемости в теже годы и рост с 2007 года
#последние годы также зеркальны предыдущиему графику, график прироста практически идентичен графику прироста по РФ.
line_data = bigdata.groupby('year', as_index=False).sum()
fig = px.line(
    data_frame=line_data, 
    x='year', 
    y=['BELG ALL rojdaemost', 'BELG ALL smertnost', 'BELG ALL prirost'],
    height=500, 
    width=1000, 
    title='Рождаемость, смертность, прирост населения Белгородская обл' 
)
fig.show()
fig.write_html("C:/Users/ikaty/OneDrive/Документы/данные/Рождаемость, смертность, прирост населения Белгородская обл.html")


# In[51]:


#график населения России показывает зависимоть общего числа населения от городского, виден некая "ямка" в период с 2000 по 2015
#при этом видно постепенно преобладание городского населения над сельским, в больших городах население растет в то время сельское падает
line_data = bigdata.groupby('year', as_index=False).sum()
fig = px.line(
    data_frame=line_data, 
    x='year', 
    y=['RU ALL', 'RU CITY', 'RU RURAL'], 
    height=600, 
    width=1000, 
    title='Население РФ общее/городское/сельское' 
)
fig.show()
fig.write_html("C:/Users/ikaty/OneDrive/Документы/данные/Население РФ общее,городское,сельское.html")


# In[52]:


#опять же схожая картина с графиком по России, общее население региона зависит от городского населения
#Городское населдение постепенно преобладает над сельским, сельское само по себе уменьшается
line_data = bigdata.groupby('year', as_index=False).sum()
fig = px.line(
    data_frame=line_data, 
    x='year', 
    y=['BELG ALL', 'BELG CITY', 'BELG RURAL'], 
    height=600, 
    width=1000, 
    title='Население Белгородская обл общее/городское/сельское' 
)
fig.show()
fig.write_html("C:/Users/ikaty/OneDrive/Документы/данные/Население Белгородская обл общее,городское,сельское.html")


# In[53]:


#Среди данных по миграции, интересна тенденция зарубежной миграции, в которой прослеживается явный как спад так и приток мигрантов
#так с 1996 по 2009 выражен спад как из РФ за рубеж так и из Белгородской обл за рубеж, а с 2010 по 216 выражен приток мигрантов
line_data = bigdata.groupby('year', as_index=False).sum()
fig = px.line(
    data_frame=line_data, 
    x='year', 
    y=['RU zarubej', 'BELG zarubej'], 
    height=600, 
    width=1000, 
    title='Зарубежняя миграция (без СНГ и внутренней миграции)' 
)
fig.show()
fig.write_html("C:/Users/ikaty/OneDrive/Документы/данные/Зарубежняя миграция (без СНГ и внутренней миграции.html")

