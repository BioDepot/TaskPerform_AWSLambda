"""
Splits a .fasta protein file into partitions of the specified size.
This expects the protein file path to be specified in proteinFilePath.
The resulting partitions will be created inside the folder specified by proteinDestinationPath.
Partition size (number of proteins per partition) is specified in proteinsPerFile.
"""
proteinFilePath = "../originalData/uniprot_humanProteinList.fasta"
proteinDestinationPath = "./proteinPartitions/"
proteinsPerFile = 500

def createFile(filePath, content, proteinCount):
    print("Creating file [" + filePath + "] with [" + str(proteinCount) + "] proteins")
    with open(filePath, "w") as destinationFile:
        destinationFile.write(content)

def main():
    with open(proteinFilePath, "r") as proteinSourceFile:
        print("Reading file [" + proteinFilePath + "] into memory...")
        proteinData = proteinSourceFile.readlines()
        print("Done reading file.")
        totalLines = len(proteinData)

        lineNumber = 0
        fileCounter = 0
        while lineNumber < totalLines:
            proteinsCounter = 0
            proteinsBuffer = ""
            while lineNumber < totalLines:
                if proteinData[lineNumber][0] == '>': #Started reading a new protein
                    if proteinsCounter == proteinsPerFile:
                        # proteinsPerFile number of proteins read and started reading a new protein.
                        break
                    proteinsCounter += 1
                proteinsBuffer += proteinData[lineNumber]
                lineNumber += 1
            fileCounter += 1
            filePath = proteinDestinationPath +"partition" + str(fileCounter) + ".fasta"
            createFile(filePath=filePath, content=proteinsBuffer, proteinCount=proteinsCounter)

main()