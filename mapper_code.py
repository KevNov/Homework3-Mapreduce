# mengimport lib
from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.runner import MRJobRunner
import re

WORD_RE = re.compile(r"[\w']+")

# membuat kelas
# kemudian mendefinisikan fungsi yang akan dipakai di mrstep methode
class MRWordFreqCount(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper, combiner=self.combiner, reducer=self.reducer)
        ]
    # mendefenisikan fungsi mapper
    # setelah itu mengekstrasi data dan menjadikan list of string sebagai outputnya
    def mapper(self, _, line):
        for word in WORD_RE.findall(line):
            yield (word.lower(), 1)
            
    # mengkombinasikan hasil iterasi data serta juga menghitung jumlah kata yang sama
    def combiner(self, word, counts):
        yield (word, sum(counts))

    # melakukan reduce output dari combiner (menggunakan iterasi yang sama seperti combiner)
    def reducer(self, word, counts):
        yield (word, sum(counts))

# melakukan fungsi eksekusi pengembangan script tanpa mempengaruhi modul lain
# kemudian memasukan data yang ingin di reduce 
if __name__ == '__main__':
    input_data = "benda.txt"
    # membuat objek mr_job dari kelas MRWordFreqCount dengan menggunakan argumen input data
    # serta juga menjalankan mr_job menggunakan lib runner
    mr_job = MRWordFreqCount(args=[input_data])
    with mr_job.make_runner() as runner:
        runner.run()
        # mendefinisikan hasil dari mr_job
        results = list(mr_job.parse_output(runner.cat_output()))
        # melakukan sorting dari mr_job dengan nilai tertinggi
        results.sort(key=lambda x: x[1], reverse=True)
        # menyimpan output mr_job dalam tipe file .txt
        with open("output.txt", "w") as f:
            for key, value in results:
                f.write(f"{key} {value}\n")
        # perintah print digunakan untuk menampilkan hasil di terminal
                print(key, value)
                