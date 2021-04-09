from mrjob.job import MRJob
from mrjob.step import MRStep


class MRcancellation(MRJob):

    def configure_args(self):
        super(MRcancellation, self).configure_args()
        self.add_file_arg('--airlines', help='Path to the airlines.csv')

    def steps(self):
        return[
            MRStep(mapper=self.mapper,
                   reducer_init=self.reducer_init,
                   reducer=self.reducer)
        ]

    def mapper(self, key, line):
        (year,month,day,day_of_week,airline,flight_number,
         tail_number,origin_airport,destination_airport,
         scheduled_departure,departure_time,departure_delay,
         taxi_out,wheels_off,scheduled_time,elapsed_time,
         air_time,distance,wheels_on,taxi_in,scheduled_arrival,
         arrival_time,arrival_delay,diverted,cancelled,
         cancellation_reason,air_system_delay,security_delay,
         airline_delay,late_aircraft_delay,weather_delay) = line.split(',')
        yield airline, int(cancelled)


    def reducer_init(self):
        self.airlines_names = {}
        with open('airlines.csv', 'r') as file:
            for line in file:
                code, name = line.split(',')
                name = name[:-1]
                self.airlines_names[code] = name


    def reducer(self, airline, cancelled):
        total_flights = 0
        total_cancellation = 0
        for c in cancelled:
            total_flights += 1
            total_cancellation += c
        yield self.airlines_names[airline], total_cancellation / total_flights

if __name__ == '__main__':
    MRcancellation.run()

