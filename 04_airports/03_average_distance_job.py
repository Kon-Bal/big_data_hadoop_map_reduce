from mrjob.job import MRJob
from mrjob.step import MRStep

class MRlights(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper
                   ,reducer=self.reducer
                   )
        ]


    def mapper(self, _, line):
        year, tekst = line.split('\t')
        year = year[1:-1]
        tekst = tekst[1:-1]
        (month, day, airline, distance) = tekst.split(', ')
        airline = airline[1:-1]
        # date = year+'-'+month+'-'+day

        yield None, (int(year),int(month), int(day), airline, int(distance))


    def reducer(self, key, values):
        total_distance = 0
        number = 0
        for value in values:
            if value[1] == 1 and value[3] == 'DL':
                total_distance += value[4]
                number += 1
        yield 'srednia odleglosc w styczniu 2015', total_distance/number

    # def reducer(self, key, values):
    #     total_distance = 0
    #     num_elements = 0
    #     for value in values:
    #         total_distance += value
    #         num_elements += 1
    #     yield None, total_distance/num_elements



if __name__ == '__main__':
    MRlights.run()