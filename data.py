import uvicorn
import dask
import logging
from fastapi import FastAPI


logging.info("program_started")
app = FastAPI()


class Data:
    def __init__(self):
        logging.info("Data object instance starting to initialize")
        self.df = None
        logging.info("Data object instance finished initializing")

    def read(self):
        logging.info("read func start - Starting reading from dask")
        self.df = dask.datasets.timeseries(start='2020-01-01', end='2020-04-01', freq="1s").compute()
        # TODO check the data was read properly try and catch clause
        logging.info("read func end - Reading from dask was finished")

    def grp_name_avg_x(self):
        logging.info("grp_name_avg_x func start")
        self.df['x_mean'] = self.df.groupby('name')['x'].transform('mean')
        logging.info("grp_name_avg_x func end - data was grouped and transformed into 'x_mean'")

    def get_name_top_10(self, name: str):
        logging.info(f"get_name_top_10 func start- name = {name}")
        df_ret = self.df.loc[self.df['name'] == name]
        logging.info("where name equals was selected")
        df_ret = df_ret.sort_values(by=['x'], ascending=False)
        logging.info("sorted descendingly according to x")
        df_ret = df_ret.head(10)
        logging.info("top 10 was selected")

        return df_ret

    def print(self):
        logging.info("print func start")
        print(self.df)
        logging.info("print func end")


@app.get("/")
def read_root():
    return {"Hello": "World"}


@app.get("/get_name_top_10/{name}")
async def get_name_top_10_fastapi(self, name: str):
    self.grp_name_avg_x()
    return self.get_name_top_10(name)

data = Data()
data.read()
data.print()
print(25 * "***")
data.grp_name_avg_x()
print(data.get_name_top_10('Patricia'))


# if __name__ == "__main__":
#     uvicorn.run(app, host="0.0.0.0", port=8000)