import matplotlib.pyplot as plt
import numpy as np

# Leer archivos de texto
def read_labels(file_path):
    with open(file_path, 'r') as file:
        labels = [line.strip() for line in file]
    return labels

# Calcular la matriz de confusión
def compute_confusion_matrix(truth, predictions):
    # Inicialización de contadores
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

# Calcular métricas
def calculate_metrics(conf_matrix):
    TP, FP = conf_matrix[0]
    FN, TN = conf_matrix[1]

    precision = TP / (TP + FP) if (TP + FP) > 0 else 0
    recall = TP / (TP + FN) if (TP + FN) > 0 else 0
    f1_score = (2 * precision * recall) / (precision + recall) if (precision + recall) > 0 else 0
    accuracy = (TP + TN) / (TP + TN + FP + FN) if (TP + TN + FP + FN) > 0 else 0
    
    return precision, recall, f1_score, accuracy

# Mostrar matriz de confusión
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

# Rutas a los archivos
truth_file = '../truth.txt'
predictions_file = '../predictions.txt'

# Leer datos
truth_labels = read_labels(truth_file)
prediction_labels = read_labels(predictions_file)

# Calcular matriz de confusión
conf_matrix = compute_confusion_matrix(truth_labels, prediction_labels)

# Calcular métricas
precision, recall, f1_score, accuracy = calculate_metrics(conf_matrix)

# Imprimir métricas
print(f'Precisión: {precision:.2f}')
print(f'Recuerdo: {recall:.2f}')
print(f'F1-Score: {f1_score:.2f}')
print(f'Exactitud: {accuracy:.2f}')

# Mostrar matriz de confusión
labels = ['Jedi', 'Sith']
plot_confusion_matrix(conf_matrix, labels)
