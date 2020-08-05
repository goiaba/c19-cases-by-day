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

    # make predictions
    train_predict = model.predict(train_x)

    day1 = model.predict(numpy.reshape(train_predict[train_predict.__len__()-1], (1, 1, 1)))
    day2 = model.predict(numpy.reshape(day1, (1, 1, 1)))
    day3 = model.predict(numpy.reshape(day2, (1, 1, 1)))
    day4 = model.predict(numpy.reshape(day3, (1, 1, 1)))
    day5 = model.predict(numpy.reshape(day4, (1, 1, 1)))
    day6 = model.predict(numpy.reshape(day5, (1, 1, 1)))
    day7 = model.predict(numpy.reshape(day6, (1, 1, 1)))
    day8 = model.predict(numpy.reshape(day7, (1, 1, 1)))
    day9 = model.predict(numpy.reshape(day8, (1, 1, 1)))
    day10 = model.predict(numpy.reshape(day9, (1, 1, 1)))

    # invert predictions
    train_predict = scaler.inverse_transform(train_predict)
    train_y = scaler.inverse_transform([train_y])

    day1 = scaler.inverse_transform(day1)
    day2 = scaler.inverse_transform(day2)
    day3 = scaler.inverse_transform(day3)
    day4 = scaler.inverse_transform(day4)
    day5 = scaler.inverse_transform(day5)
    day6 = scaler.inverse_transform(day6)
    day7 = scaler.inverse_transform(day7)
    day8 = scaler.inverse_transform(day8)
    day9 = scaler.inverse_transform(day9)
    day10 = scaler.inverse_transform(day10)

    output = [
        day1.tolist()[0][0],
        day2.tolist()[0][0],
        day3.tolist()[0][0],
        day4.tolist()[0][0],
        day5.tolist()[0][0],
        day6.tolist()[0][0],
        day7.tolist()[0][0],
        day8.tolist()[0][0],
        day9.tolist()[0][0],
        day10.tolist()[0][0],
    ]


    print(trainPredict)
    print(output)
