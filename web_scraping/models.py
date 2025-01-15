from dataclasses import dataclass

@dataclass
class Movie:
    title: str
    date: str
    rating: str
    plot: str

    def to_csv_row(self):
        return [self.title, self.date, self.rating, self.plot] 