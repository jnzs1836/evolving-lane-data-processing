import torch.nn as nn
import torch.nn.functional as F
import torch.optim as optim


DIMENSION = 50

class EmbeddingModel(nn.Module):
    def __init__(self,edge_num,embedding_dim):
        super(EmbeddingModel,self).__init__()
        self.embedding_layer = nn.Embedding(edge_num,embedding_dim )
    def forward(self, *input):
        return

