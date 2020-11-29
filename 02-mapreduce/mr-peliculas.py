from mrjob.job import MRJob
from mrjob.step import MRStep
from datetime import datetime

class MayorValorAccion(MRJob):

    def mapper(self, _, line):
        line = line.split(',')
        company = line[0]
        price_date = {
            "price":float(line[1]),
            "date": line[2]
        }
        yield company,price_date

    def reducer(self, key, values):
        date = None
        higherPrice = 0
        for value in values:
            if(value["price"]>higherPrice):
                higherPrice = value["price"]
                date = value["date"]

        yield key, (date, higherPrice)

class MenorValorAccion(MRJob):

    def mapper(self, _, line):
        line = line.split(',')
        company = line[0]
        price_date = {
            "price":float(line[1]),
            "date": line[2]
        }
        yield company,price_date

    def reducer(self, key, values):
        date = None
        lowerPrice = None
        for value in values:
            if(lowerPrice == None):
                lowerPrice = value["price"]
                date = value["date"]
            if(value["price"]<lowerPrice):
                lowerPrice = value["price"]
                date = value["date"]

        yield key, (date, lowerPrice)

class SubidaDeAcciones(MRJob):

    def mapper(self, _, line):
        line = line.split(',')
        company = line[0]
        price_date = {
            "price":float(line[1]),
            "date": line[2]
        }
        yield company,price_date

    def reducer(self, key, values):
        val = sorted(
            values,
            key=lambda x: datetime.strptime(x['date'], '%Y-%m-%d'), reverse=False
        )
        isGrowing = True
        for i in range(1, len(val)):
            if(val[i-1]["price"] > val[i]["price"]):
                isGrowing = False
                break
        if(isGrowing):
            yield key, None

class DiaNegro(MRJob):

    def mapper(self, _, line):
        line = line.split(',')
        price = float(line[1])
        date = line[2]
        yield date,price

    def reducer(self, key, values):
        val = list(values)
        yield None, (sum(val), key)
    
    def reducer_find_max(self, _, values):
        yield max(values)

    def steps(self):
        return [
            MRStep(
                mapper=self.mapper,
                reducer=self.reducer
            ),
            MRStep(
                reducer=self.reducer_find_max
            )
        ]

if __name__ == '__main__':
    MayorValorAccion.run()
    MenorValorAccion.run()
    SubidaDeAcciones.run()
    DiaNegro.run()