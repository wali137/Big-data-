from sklearn.linear_model import Perceptron
from sklearn.neighbors import KNeighborsClassifier


data = []
test = []
classes = []
answers = []

training_file = "vertigo_train.txt"
test_file = "vertigo_predict.txt"
answer_file = "vertigo_answers.txt"

training_data = [line.rstrip('\n') for line in open(training_file)]
test_data = [line.rstrip('\n') for line in open(test_file)]
answer_data = [line.rstrip('\n') for line in open(answer_file)]

answers = map(int,answer_data)

for instances in training_data:
    instance = map(int,instances.split(" "))
    classes.append(instance[0])
    instance.pop(0)
    data.append(instance)
    
for instances in test_data:
    instance = map(int,instances.split(" "))
    test.append(instance)



#Preceptron Classifer
p = Perceptron()
p.fit(data, classes)
p_predictions =  p.predict(test)

p_correctly_classified = 0
p_incorrectly_classified = 0

for i in range(0,len(p_predictions)):
    if answers[i] == int(p_predictions[i]):
        p_correctly_classified += 1
    else:
        p_incorrectly_classified +=1


#KNN Classifer
neigh = KNeighborsClassifier(p=1) #here K is 5 by default and p=1 for Manhattan Distance
neigh.fit(data,classes)
k_predictions = neigh.predict(test)

k_correctly_classified = 0
k_incorrectly_classified = 0

for i in range(0,len(k_predictions)):
    if answers[i] == int(k_predictions[i]):
        k_correctly_classified += 1
    else:
        k_incorrectly_classified +=1



print "Perceptron: %s %% correct" % str(round((float(p_correctly_classified)*100.0)/float(len(test)),2))
print "Nearest neighbor: %s %% correct" % str(round((float(k_correctly_classified)*100.0)/float(len(test)),2))