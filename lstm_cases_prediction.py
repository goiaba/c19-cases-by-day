import json
import os
import numpy
import pandas
from config import PREDICTION
from datetime import timedelta
from keras.models import Sequential
from keras.layers import Dense
from keras.layers import LSTM
from sklearn.preprocessing import MinMaxScaler
from lstm_dao import LstmDao
from util import jhustr_to_mdbstr, mdbstr_to_time, time_to_mdbstr


# convert an array of values into a dataset matrix
def create_dataset(ds, lb=1):
    data_x, data_y = [], []
    for i in range(len(ds) - lb - 1):
        a = ds[i:(i + lb), 0]
        data_x.append(a)
        data_y.append(ds[i + lb, 0])
    return numpy.array(data_x), numpy.array(data_y)


def get_data():
    import requests
    response = requests.get(PREDICTION.get("url"))
    if response.status_code == 200:
        return json.loads(response.text)[0]
    return {}


def get_covid_cases_from_first_case():
    covid_cases = {jhustr_to_mdbstr(k): v.get('confirmed', 0) for k, v in get_data().get('timeseries', {}).items()}
    for cases_date in list(covid_cases.keys()):
        if covid_cases[cases_date] == 0:
            del covid_cases[cases_date]
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
    increment = 1 / train_predict.size
    day = 1

    while bool(model.predict(numpy.reshape(day, (1, 1, 1))) < 1):
        day += increment

    predictions = []
    entrance_date_str = dataframe.index.max()
    date_prediction = mdbstr_to_time(entrance_date_str)
    for i in range(PREDICTION.get("days_to_predict", 10)):
        date_prediction = date_prediction + timedelta(days=1)
        predictions.append({
            "idCountry": PREDICTION.get("idCountry"),
            "predictedCases": scaler.inverse_transform(model.predict(numpy.reshape(day, (1, 1, 1)))).tolist()[0][0],
            "datePrediction": time_to_mdbstr(date_prediction),
            "entranceDate": entrance_date_str
        })
        day += increment

    db_handler = LstmDao(host=os.environ.get("DB_HOST", "localhost"), database=os.environ.get("DB_NAME", "covid"),
                         user=os.environ.get("DB_USER", "root"), password=os.environ.get("DB_PASS", "root"))
    db_handler.delete_cases_prediction_if_exists(entrance_date_str)
    db_handler.persist_cases_prediction(predictions)
