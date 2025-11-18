# Assignment 1 - Big Data: TF-IDF with Hadoop MapReduce

This project implements a complete TF-IDF (Term Frequency - Inverse Document Frequency) computation pipeline using Hadoop MapReduce in Java. The pipeline processes a corpus of text documents to compute TF-IDF scores and identify the top-N most significant terms per document.

---

## Overview

The pipeline computes:
- **Raw Term Counts**: f(t,d) - frequency of term t in document d
- **Term Frequency (TF)**: tf(t,d) = f(t,d) / Σ f(t',d)
- **Inverse Document Frequency (IDF)**: idf(t) = log(N / df(t))
- **TF-IDF Scores**: tfidf(t,d) = tf(t,d) × idf(t)
- **Top-N Terms**: The N terms with highest TF-IDF scores per document

---

## Mathematical Definitions

Let:
- **t** = a term (token/word)
- **d** = a document
- **D** = the corpus (collection of all documents)
- **f(t,d)** = raw count of term t in document d
- **N** = total number of documents in the corpus

### Formulas

**Term Frequency (normalized by document length)**:
```
tf(t,d) = f(t,d) / Σ_{t' ∈ d} f(t',d)
```

**Inverse Document Frequency (standard definition)**:
```
idf(t) = log(N / df(t))
```
where df(t) = number of documents containing term t

**TF-IDF**:
```
tfidf(t,d) = tf(t,d) × idf(t)
```

---

## Project Layout

```
Assignment1_BigData/
├── code/
│   ├── pom.xml
│   ├── src/main/java/sn1/assignment1_bigdata/
│   │   ├── Driver.java
│   │   ├── Tokenizer.java
│   │   ├── Job1_TermCount.java
│   │   ├── Job2_TF.java
│   │   ├── Job3_IDF.java
│   │   ├── Job4_TFIDF.java
│   │   ├── Job5_TopNTFIDF.java
│   │   
│   └── target/
│       └── assignment1-bigdata-1.0-SNAPSHOT.jar
└── inputs/
    ├── input1.txt                    # Sample document 1
    ├── input2.txt                    # Sample document 2
    └── input3.txt                    # Sample document 3
```

## Environment Setup

### Requirements
- **Hadoop**: 3.3.1 or compatible
- **Java**: 11 (as configured in pom.xml)
- **Maven**: 3.6+
- **Operating System**: Linux/WSL recommended

### Hadoop Dependencies
The project uses Hadoop 3.3.1 libraries (provided scope):
- `hadoop-common`
- `hadoop-mapreduce-client-core`

---

## Building the Project

```bash
cd code
mvn clean package
```

This produces: `target/assignment1-bigdata-1.0-SNAPSHOT.jar`

The JAR manifest specifies `sn1.assignment1_bigdata.Driver` as the main class, so you don't need to specify it when running.

---

## Preparing Input Data

### Upload Sample Data to HDFS

The project includes sample documents in the `inputs/` folder. Upload them to HDFS:

```bash
# Create HDFS input directory
hadoop fs -mkdir -p /user/$(whoami)/tfidf_input

# Upload sample documents
hadoop fs -put inputs/* /user/$(whoami)/tfidf_input/

# Verify upload
hadoop fs -ls /user/$(whoami)/tfidf_input
```

Expected output:
```
Found 3 items
-rw-r--r--   1 user supergroup   ... /user/user/tfidf_input/input1.txt
-rw-r--r--   1 user supergroup   ... /user/user/tfidf_input/input2.txt
-rw-r--r--   1 user supergroup   ... /user/user/tfidf_input/input3.txt
```

### Using Custom Input Files

To use your own documents:
```bash
# Upload from local directory
hadoop fs -put /path/to/your/documents/* /user/$(whoami)/tfidf_input/

# Or upload individual files
hadoop fs -put mydoc.txt /user/$(whoami)/tfidf_input/
```

**Note**: Each file in the input directory is treated as a separate document. The filename becomes the document ID used in output.

---

## Running the Pipeline

### Basic Usage

```bash
hadoop jar target/assignment1-bigdata-1.0-SNAPSHOT.jar <input_path> <output_path> [topN]
```

**Arguments**:
- `<input_path>`: HDFS path to input documents directory
- `<output_path>`: HDFS path for output (will be created)
- `[topN]`: Optional - number of top terms to extract per document (default: 5)

### Example

```bash
# Clean previous output
hadoop fs -rm -r -f /user/hadoop/tfidf_output

# Run with default top-5
hadoop jar target/assignment1-bigdata-1.0-SNAPSHOT.jar \
  /user/hadoop/tfidf_input \
  /user/hadoop/tfidf_output

# Run with custom top-10
hadoop jar target/assignment1-bigdata-1.0-SNAPSHOT.jar \
  /user/hadoop/tfidf_input \
  /user/hadoop/tfidf_output \
  10
```

---

## Pipeline Stages

### Job 1: Term Count (Job1_TermCount.java)
**Purpose**: Count term occurrences and total terms per document

**Mapper**:
- Input: Each line of each document
- Emits:
  - `("term@docId", 1)` for each term occurrence
  - `("__DOC__@docId", 1)` for each token (to count doc totals)

**Reducer**:
- For `term@doc`: sums to produce `f(t,d)`
- For `__DOC__@doc`: sums to produce document total, outputs as `DOC_TOTAL@docId`

**Output Format**: `term@docId<TAB>count` or `DOC_TOTAL@docId<TAB>total`

---

### Job 2: TF Computation (Job2_TF.java)
**Purpose**: Calculate normalized term frequency

**Mapper**:
- Parses Job1 output
- Groups by document ID
- Emits:
  - Key: `docId`
  - Value: `"T\tterm\tf"` for term counts
  - Value: `"D\tTOTAL\tF"` for document totals

**Reducer**:
- Performs reduce-side join
- Computes: `tf = f(t,d) / DOC_TOTAL(d)`
- Outputs: `term@docId<TAB>tf`

**Output Format**: `term@docId<TAB>tf_score`

---

### Job 3: IDF Computation (Job3_IDF.java)
**Purpose**: Calculate inverse document frequency

**Mapper**:
- Reads Job1 output (term counts)
- Emits: `(term, 1)` for each term@doc occurrence
- Ignores `DOC_TOTAL` lines

**Reducer**:
- Counts df(t) = number of documents containing term t
- Retrieves N from configuration (set by Driver after counting input files)
- Computes: `idf = log(N / df(t))`
- Outputs: `term<TAB>idf`

**Output Format**: `term<TAB>idf_score`

---

### Job 4: TF-IDF Computation (Job4_TFIDF.java)
**Purpose**: Join TF and IDF to compute TF-IDF scores

**Mapper**:
- Reads both TF output (step2) and IDF output (step3)
- For TF lines (`term@doc`): emits `(term, "TF\tdoc\ttf")`
- For IDF lines (`term`): emits `(term, "IDF\tidf")`

**Reducer**:
- Performs reduce-side join on term
- Multiplies: `tfidf = tf × idf`
- Outputs: `term@docId<TAB>tfidf`

**Output Format**: `term@docId<TAB>tfidf_score`

---

### Job 5: Top-N Selection (Job5_TopNTFIDF.java)
**Purpose**: Extract top-N terms with highest TF-IDF per document

**Mapper**:
- Parses `term@doc` format
- Emits: key=`docId`, value=`"term\ttfidf"`

**Reducer**:
- Uses min-heap (PriorityQueue) of size N
- Keeps only the N highest TF-IDF scores
- Sorts in descending order
- Outputs: `docId<TAB>term\ttfidf` (one line per top term)

**Output Format**: `docId<TAB>term<TAB>tfidf_score`

---

## Output Structure

After successful execution, the output directory contains:

```
<output_path>/
├── step1_termcount/     # Raw term counts and document totals
│   └── part-r-00000
├── step2_tf/            # Term frequency scores
│   └── part-r-00000
├── step3_idf/           # Inverse document frequency scores
│   └── part-r-00000
├── step4_tfidf/         # TF-IDF scores for all terms
│   └── part-r-00000
└── step5_topn_tfidf/    # Top-N terms per document
    └── part-r-00000
```

### Viewing Results

```bash
# View step 1: term counts
hadoop fs -cat <output_path>/step1_termcount/part-r-00000 | head -20

# View step 2: TF scores
hadoop fs -cat <output_path>/step2_tf/part-r-00000 | head -20

# View step 3: IDF scores
hadoop fs -cat <output_path>/step3_idf/part-r-00000 | head -20

# View step 4: TF-IDF scores
hadoop fs -cat <output_path>/step4_tfidf/part-r-00000 | head -20

# View top-N results (sorted by document)
hadoop fs -cat <output_path>/step5_topn_tfidf/part-r-00000 | sort
```

---

## Tokenization

The `Tokenizer.java` class performs:
1. **Lowercase conversion**: All text converted to lowercase
2. **Non-alphanumeric removal**: Replaces `[^a-z0-9\s]` with spaces
3. **Whitespace splitting**: Splits on `\s+`
4. **Empty token filtering**: Removes empty strings

**Example**:
```
Input:  "Hello, World! This is MapReduce."
Tokens: ["hello", "world", "this", "is", "mapreduce"]
```

### Customizing Tokenization

To add stopword filtering or other preprocessing:
1. Edit `src/main/java/sn1/assignment1_bigdata/Tokenizer.java`
2. Modify the `tokenize()` method
3. Rebuild: `mvn clean package`

---

## Troubleshooting

### Common Issues

**1. ClassNotFoundException**
```
Solution: Ensure the JAR manifest contains the correct main class.
Verify: jar xf target/assignment1-bigdata-1.0-SNAPSHOT.jar META-INF/MANIFEST.MF
Check: grep Main-Class META-INF/MANIFEST.MF
```

**2. Output directory already exists**
```
Error: org.apache.hadoop.mapred.FileAlreadyExistsException
Solution: Delete output directory before running
Command: hadoop fs -rm -r -f <output_path>
```

**3. No input files found**
```
Error: Input path does not exist
Solution: Verify input path exists and contains files
Commands:
  hadoop fs -ls <input_path>
  hadoop fs -put local_files/* <input_path>/
```

**4. Zero documents counted**
```
Issue: Driver.countDocs() returns 0
Solution: Ensure input path contains actual files (not just directories)
Check: hadoop fs -ls <input_path>
```

**5. Missing IDF values**
```
Issue: Some terms have no IDF in step3 output
Cause: Term doesn't appear in Job1 output
Solution: Verify Job1 completed successfully and produced output
```

---

## Performance Considerations

### For Small Datasets (< 100 documents)
- Default configuration works well
- Single reducer per job is sufficient

### For Large Datasets (> 1000 documents)
Consider modifying jobs to add:
```java
job.setNumReduceTasks(N);  // Increase reducer count
```

Recommended reducer counts:
- Job1 (TermCount): 5-10 reducers
- Job2 (TF): 5-10 reducers
- Job3 (IDF): 2-5 reducers
- Job4 (TFIDF): 5-10 reducers
- Job5 (TopN): 2-5 reducers

### Memory Settings
For large documents, increase mapper/reducer memory:
```bash
hadoop jar target/assignment1-bigdata-1.0-SNAPSHOT.jar \
  -Dmapreduce.map.memory.mb=2048 \
  -Dmapreduce.reduce.memory.mb=4096 \
  <input_path> <output_path>
```

---

## Validation

### Sanity Checks

1. **TF values should sum to ≈1 per document**:
```bash
hadoop fs -cat <output>/step2_tf/part-r-00000 | \
  awk -F'@|\t' '{doc=$2; sum[doc]+=$3} END {for(d in sum) print d, sum[d]}'
```

2. **IDF should be positive**:
```bash
hadoop fs -cat <output>/step3_idf/part-r-00000 | \
  awk '{if($2<=0) print "ERROR:", $0}'
```

3. **Document count matches input**:
Check Driver console output for: `Detected number of documents: N`

4. **Top-N output has correct count per doc**:
```bash
hadoop fs -cat <output>/step5_topn_tfidf/part-r-00000 | \
  awk '{print $1}' | uniq -c
```

## Academic Context

This implementation demonstrates key MapReduce patterns:
- **Multi-stage pipeline**: Chaining multiple MapReduce jobs
- **Reduce-side joins**: Combining data by common keys (Job2, Job4)
- **Configuration passing**: Sharing metadata (document count) across jobs
- **Top-K selection**: Using min-heap for efficient ranking (Job5)
- **Data denormalization**: Using composite keys (`term@doc`) for grouping

---

## License

Academic use for Big Data coursework. Adapt as needed for educational purposes.

---
