import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.io.Writable;

public class Point implements Writable{
    private float[] data = new float[2];

    public Point(){
        data[0] = 0;
        data[1] = 0;
    }

    public Point(final float[] a){
        if(a.length == 2){
            data[0] = a[0];
            data[1] = a[1];
        }
    }

    public Point(final float a, final float b){
        this.data[0] = a;
        this.data[1] = b;
    }

    public Point(final String[] s){
        if(s.length == 2){
            data[0] = Float.parseFloat(s[0]);
            data[1] = Float.parseFloat(s[1]);
        }
    }

    public static Point copy(final Point a){
        Point temp = new Point(a.data);
        return temp;
    }

    public String getString(final char delim){
        return String.valueOf(data[0])+delim+String.valueOf(data[1]);
    }

    public float distance_sqr(final Point other){
        // we use Euclidean here: i^2
        // Comment: I hate Java's double-float casting, compared to C.
        float a = this.data[0] - other.data[0];
        float b = this.data[1] - other.data[1];
        return a*a + b*b;
    }

    public boolean equalFloats(final float a, final float b){
        return Math.abs(a - b) < 0.00001F;
    }

    public boolean equal(final Point other){
        return equalFloats(this.data[0], other.data[0]) &&
        equalFloats(this.data[1], other.data[1]);
    }

    public static Point randomPoint_inCircle(final Point center, final float rad){
        float rad_2 = rad * rad;
        Random rand = new Random();
        float x = rand.nextFloat() * rad;

        double temp = rad_2 - x*x;
        float y = (float) Math.sqrt(temp) * rand.nextFloat();

        return new Point(x, y);
    }

    public void add(final Point other){
        this.data[0] = this.data[0] + other.data[0];
        this.data[1] = this.data[1] + other.data[1];
    }
    
    public void divide(final float a){
        this.data[0] /= a;
        this.data[1] /= a;
    }

    @Override
    public void write(final DataOutput out) throws IOException {
        out.writeFloat(this.data[0]);
        out.writeChar(' ');
        out.writeFloat(this.data[1]);
    }

    @Override
    public void readFields(final DataInput in) throws IOException {
        float x = in.readFloat();
        float y = in.readFloat();

        this.data[0] = x;
        this.data[1] = y;
    }
}