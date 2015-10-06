#!/bin/bash

make

if [ ! -d "$2" ]; then
  mkdir $2
fi

CORPUS=$1
VOCAB_FILE=$2/vocab.txt
COOCCURRENCE_FILE=$2/cooccurrence.bin
COOCCURRENCE_SHUF_FILE=$2/cooccurrence.shuf.bin
SAVE_FILE=$2/vectors
VERBOSE=2
MEMORY=256.0
VOCAB_MIN_COUNT=0
VECTOR_SIZE=100
MAX_ITER=15
WINDOW_SIZE=15
BINARY=2
NUM_THREADS=12
X_MAX=10

./vocab_count -min-count $VOCAB_MIN_COUNT -verbose $VERBOSE < $CORPUS > $VOCAB_FILE
if [[ $? -eq 0 ]]
  then
  ./cooccur -memory $MEMORY -vocab-file $VOCAB_FILE -verbose $VERBOSE -window-size $WINDOW_SIZE < $CORPUS > $COOCCURRENCE_FILE
  if [[ $? -eq 0 ]]
  then
    ./shuffle -memory $MEMORY -verbose $VERBOSE < $COOCCURRENCE_FILE > $COOCCURRENCE_SHUF_FILE
    if [[ $? -eq 0 ]]
    then
       ./glove -save-file $SAVE_FILE -threads $NUM_THREADS -input-file $COOCCURRENCE_SHUF_FILE -x-max $X_MAX -iter $MAX_ITER -vector-size $VECTOR_SIZE -binary $BINARY -vocab-file $VOCAB_FILE -verbose $VERBOSE
    fi
  fi
fi


