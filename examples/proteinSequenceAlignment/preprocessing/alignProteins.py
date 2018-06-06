"""
Runs pair-wise SSW on all protein partitions.
This assumes:
 - partitionProteins.py was already run, the partitions are located in ./proteinPartitions
 - ssw_test is in the same folder as this (./)
 - there is a folder ./results/alignment in which the resulting alignments will go

To log errors you can run me with:
python3 -u alignProteins.py 2>&1 | tee align.log
"""

import time
import subprocess
fileCount = 41
filePath = "./proteinPartitions/"

def main():
    for i in range(1, fileCount + 1):
        for j in range(i, fileCount + 1):
                print("Starting alignment[" + str(i) + ", " + str(j) + "] at " + str(int(round(time.time() * 1000))) + " ms.")
                (output, err) = subprocess.Popen("./ssw_test -p ./proteinPartitions/partition" + str(i) + 
                  ".fasta ./proteinPartitions/partition" + str(j) + ".fasta ./BLOSUM62 -o 10 -e 1 -l  > ./results/alignment" +
                  str(i) + "-" + str(j), stdout=None, stderr=subprocess.PIPE, shell=True).communicate()
                if err:
                    print(err)

main()
