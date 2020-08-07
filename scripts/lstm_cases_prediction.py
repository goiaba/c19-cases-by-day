import json
import os
import numpy
import matplotlib.pyplot as plt
import math
import pandas
from keras.models import Sequential
from keras.layers import Dense
from keras.layers import LSTM
from sklearn.preprocessing import MinMaxScaler
from sklearn.metrics import mean_squared_error
from lstm_dao import LstmDao


# convert an array of values into a dataset matrix
def create_dataset(ds, lb=1):
    data_x, data_y = [], []
    for i in range(len(ds) - lb - 1):
        a = ds[i:(i + lb), 0]
        data_x.append(a)
        data_y.append(ds[i + lb, 0])
    return numpy.array(data_x), numpy.array(data_y)


def get_data():
    url = 'https://wuhan-coronavirus-api.laeyoung.endpoint.ainize.ai/jhu-edu/timeseries?iso2=BR'
    import requests
    response = requests.get(url)
    if response.status_code == 200:
        return json.loads(response.text)[0]
    return {}


def get_covid_cases_from_first_case():
    covid_cases = {k: v.get('confirmed', 0) for k, v in get_data().get('timeseries', {}).items()}
    for entranceDate in list(covid_cases.keys()):
        if covid_cases[entranceDate] == 0:
            del covid_cases[entranceDate]
        else:
            break
    return covid_cases


if __name__ == "__main__":
    # fix random seed for reproducibility
    numpy.random.seed(7)

    # load the dataset
    dataframe = pandas.DataFrame.from_dict(get_covid_cases_from_first_case(), orient='index', columns=['cases'])
    dataset = dataframe.filter(['cases'], axis=1).values
    dataset = dataset.astype('float32')

    # normalize the dataset
    scaler = MinMaxScaler(feature_range=(0, 1))
    dataset = scaler.fit_transform(dataset)

    # split into train and test sets
    train_size = int(len(dataset))
    train = dataset

    # reshape into X=t and Y=t+1
    look_back = 1
    train_x, train_y = create_dataset(train, look_back)


    # reshape input to be [samples, time steps, features]
    train_x = numpy.reshape(train_x, (train_x.shape[0], 1, train_x.shape[1]))


    # create and fit the LSTM network
    model = Sequential()
    model.add(LSTM(4, input_shape=(1, look_back)))
    model.add(Dense(1))
    model.compile(loss='mean_squared_error', optimizer='adam')
    model.fit(train_x, train_y, epochs=100, batch_size=1, verbose=2)

    times = 10
    output = []
    # make predictions
    trainPredict = model.predict(train_x)
    increment = 1 / trainPredict.size
    day = 1

    while bool(model.predict(numpy.reshape(day, (1, 1, 1))) < 1):
        day += increment

    for i in range(times):
        output.append(scaler.inverse_transform(model.predict(numpy.reshape(day, (1, 1, 1)))).tolist()[0][0])
        day += increment

    # invert predictions
    trainPredict = scaler.inverse_transform(trainPredict)
    trainY = scaler.inverse_transform([train_y])

    print(trainPredict)
    print(output)
