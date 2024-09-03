import matplotlib.pyplot as plt
import numpy as np


def read_labels(file_path):
    with open(file_path, 'r') as file:
        labels = [line.strip() for line in file]
    return labels


def compute_confusion_matrix(truth, predictions):
    TP = FP = FN = TN = 0
    
    for t, p in zip(truth, predictions):
        if t == 'Jedi' and p == 'Jedi':
            TP += 1
        elif t == 'Jedi' and p == 'Sith':
            FN += 1
        elif t == 'Sith' and p == 'Jedi':
            FP += 1
        elif t == 'Sith' and p == 'Sith':
            TN += 1
    
    return np.array([[TP, FP], [FN, TN]])


def calculate_metrics(conf_matrix):
    TP, FP = conf_matrix[0]
    FN, TN = conf_matrix[1]

    precision = TP / (TP + FP) if (TP + FP) > 0 else 0
    recall = TP / (TP + FN) if (TP + FN) > 0 else 0
    f1_score = (2 * precision * recall) / (precision + recall) if (precision + recall) > 0 else 0
    accuracy = (TP + TN) / (TP + TN + FP + FN) if (TP + TN + FP + FN) > 0 else 0
    
    return precision, recall, f1_score, accuracy


def plot_confusion_matrix(conf_matrix, labels):
    fig, ax = plt.subplots()
    cax = ax.matshow(conf_matrix, cmap='Blues')
    plt.colorbar(cax)
    
    ax.set_xticklabels([''] + labels)
    ax.set_yticklabels([''] + labels)
    
    for (i, j), val in np.ndenumerate(conf_matrix):
        ax.text(j, i, int(val), ha='center', va='center', color='black')
    
    plt.xlabel('Predicción')
    plt.ylabel('Real')
    plt.title('Matriz de Confusión')
    plt.show()


if __name__ == "__main__" :
        
    truth_file = '../truth.txt'
    predictions_file = '../predictions.txt'

    truth_labels = read_labels(truth_file)
    prediction_labels = read_labels(predictions_file)

    conf_matrix = compute_confusion_matrix(truth_labels, prediction_labels)

    precision, recall, f1_score, accuracy = calculate_metrics(conf_matrix)

    print(f'Precisión: {precision:.2f}')
    print(f'Recuerdo: {recall:.2f}')
    print(f'F1-Score: {f1_score:.2f}')
    print(f'Exactitud: {accuracy:.2f}')

    labels = ['Jedi', 'Sith']
    plot_confusion_matrix(conf_matrix, labels)
