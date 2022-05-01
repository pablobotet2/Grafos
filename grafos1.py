from pyspark import SparkContext
import sys

def mapper(line):
    edge = line.split(',')
    n1 = edge[0]
    n2 = edge[1]
    return [(n1,n2), (n2,n1)]

def mapper2(elem):
    if elem[0]>elem[1]:
        return (elem[1],elem[0])
    else:
        return elem
        
def posibilidades(elem):
    result=[]
    for i in range(len(elem[1])):
        result.append(((elem[0],elem[1][i]),'exists'))
    num= len(result)
    for i in range(num-1):
        for j in range(i+1,num):  
            result.append(((result[i][0][1],result[j][0][1]),('pending',elem[0])))
    return result
 
def triciclo(lista):
    result=[]
    for i in range(len(lista)):
        if lista[i][1][0]=='pending':#hacemos esto para que solamente busque para los que están pe
            for j in range(i+1,len(lista)):
                if lista[j][1]=='exists' and lista[j][0]==lista[i][0]:
                    result.append((lista[i][1][1],lista[i][0][0],lista[i][0][1]))
    return result
    
SAMPLE = 15
sc = SparkContext()

rdd = sc.textFile(sys.argv[1])
print('textFile', rdd.take(SAMPLE))

rdd = rdd.flatMap(mapper)
print('flatMap', rdd.take(SAMPLE))

rdd = rdd.filter(lambda x: x[0]!=x[1])
print('filter', rdd.take(SAMPLE))

rdd = rdd.map(mapper2)
print('menores_primero',rdd.take(SAMPLE))

rdd = rdd.distinct()
print('distinct', rdd.take(SAMPLE))

rdd = rdd.groupByKey()
print('groupByKey', rdd.take(SAMPLE))

rdd = rdd.map(lambda x: (x[0],tuple(sorted(x[1]))))
rdd =rdd.flatMap(posibilidades)
print('prueba',rdd.take(SAMPLE))
print('Result:')
lista=triciclo(rdd.collect())
print(lista)

