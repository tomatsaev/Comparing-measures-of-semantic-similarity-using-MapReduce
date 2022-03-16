package helpers;

import associations.Associations;
import lombok.AllArgsConstructor;
import lombok.Data;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
@Data
@AllArgsConstructor(staticName = "of")
public class TaggedPair implements WritableComparable<TaggedPair> {
    Associations.Tag tag;
    Text text;

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        WritableUtils.writeString(dataOutput, tag.toString());
        text.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        tag = Associations.Tag.valueOf(WritableUtils.readString(dataInput));
        text.readFields(dataInput);
    }

    @Override
    public int compareTo(TaggedPair o) {
        if (tag.compareTo(o.tag) == 0)
            return text.compareTo(o.text);
        return tag.compareTo(o.tag);
    }
}