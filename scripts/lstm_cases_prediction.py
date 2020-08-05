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
    train_size = int(len(dataset) * 0.67)
    test_size = len(dataset) - train_size
    train, test = dataset[0:train_size, :], dataset[train_size:len(dataset), :]

    # reshape into X=t and Y=t+1
    look_back = 1
    train_x, train_y = create_dataset(train, look_back)
    test_x, test_y = create_dataset(test, look_back)

    # reshape input to be [samples, time steps, features]
    train_x = numpy.reshape(train_x, (train_x.shape[0], 1, train_x.shape[1]))
    test_x = numpy.reshape(test_x, (test_x.shape[0], 1, test_x.shape[1]))

    # create and fit the LSTM network
    model = Sequential()
    model.add(LSTM(4, input_shape=(1, look_back)))
    model.add(Dense(1))
    model.compile(loss='mean_squared_error', optimizer='adam')
    model.fit(train_x, train_y, epochs=100, batch_size=1, verbose=2)

    # make predictions
    train_predict = model.predict(train_x)
    test_predict = model.predict(test_x)

    # invert predictions
    train_predict = scaler.inverse_transform(train_predict)
    train_y = scaler.inverse_transform([train_y])
    test_predict = scaler.inverse_transform(test_predict)
    test_y = scaler.inverse_transform([test_y])

    # calculate root mean squared error
    train_score = math.sqrt(mean_squared_error(train_y[0], train_predict[:, 0]))
    print('Train Score: %.2f RMSE' % train_score)
    test_score = math.sqrt(mean_squared_error(test_y[0], test_predict[:, 0]))
    print('Test Score: %.2f RMSE' % test_score)

    # shift train predictions for plotting
    train_predict_plot = numpy.empty_like(dataset)
    train_predict_plot[:, :] = numpy.nan
    train_predict_plot[look_back:len(train_predict) + look_back, :] = train_predict

    # shift test predictions for plotting
    test_precidt_plot = numpy.empty_like(dataset)
    test_precidt_plot[:, :] = numpy.nan
    test_precidt_plot[len(train_predict) + (look_back * 2) + 1:len(dataset) - 1, :] = test_predict

    # plot baseline and predictions
    plt.plot(scaler.inverse_transform(dataset))
    plt.plot(train_predict_plot)
    plt.plot(test_precidt_plot)
    plt.show()
