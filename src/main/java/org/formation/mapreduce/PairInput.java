package org.formation.mapreduce;

import org.apache.hadoop.io.Text;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class PairInput implements WritableComparable<PairInput> {

    private Text mot;
    private Text doc_id;

    public PairInput(Text mot,Text doc_id) {
        this.mot=mot;
        this.doc_id=doc_id;
    }

    public PairInput(String mot, String doc_id) {
        this.mot=new Text(mot);
        this.doc_id=new Text(doc_id);

    }
    public PairInput() {
        this.mot=new Text();
        this.doc_id=new Text();
    }

    public Text getMot() { return mot; }

    public void setMot(Text mot) { this.mot = mot;    }

    public Text getDoc_id() { return doc_id;    }

    public void setDoc_id(Text doc_id) { this.doc_id = doc_id; }

    public void write(DataOutput out) throws IOException {
        mot.write(out);
        doc_id.write(out);
    }
    public void readFields(DataInput in) throws IOException {
        mot.readFields(in);
        doc_id.readFields(in);
    }
    public String toString() {
        return mot.toString() + " " + doc_id.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PairInput pairInput = (PairInput) o;
        return Objects.equals(mot, pairInput.mot) &&
                Objects.equals(doc_id, pairInput.doc_id);
    }

    public int compareTo(PairInput o) {
        int cmp =doc_id.compareTo(o.getDoc_id());

        if (cmp != 0) {
            return cmp;
        }


        return  mot.compareTo(o.getMot());
    }
}
