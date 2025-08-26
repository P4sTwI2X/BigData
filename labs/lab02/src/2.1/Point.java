import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.io.Writable;

public class Point implements Writable{
    private float X, Y;

    public Point(){
        this.X = 0.0f;
        this.Y = 0.0f;
    }

    public Point(final float[] a){
        if(a.length == 2){
            this.X = a[0];
            this.Y = a[1];
        }
    }

    public Point(final float a, final float b){
        this.X = a;
        this.Y = b;
    }

    public Point(final String[] s){
        if(s.length == 2){
            this.X = Float.parseFloat(s[0]);
            this.Y = Float.parseFloat(s[1]);
        }
    }

    public static Point copy(final Point a){
        Point temp = new Point(a.X, a.Y);
        return temp;
    }

    public String getString(final char delim){
        return String.valueOf(this.X) + delim + String.valueOf(this.Y);
    }

    public float distance_sqr(final Point other){
        // we use Euclidean here: i^2
        // Comment: I hate Java's double-float casting, compared to C.
        float a = this.X - other.X;
        float b = this.Y - other.Y;
        return a*a + b*b;
    }

    public boolean equalfloats(final float a, final float b){
        return Math.abs(a - b) < 0.00001F;
    }

    public boolean equal(final Point other){
        return equalfloats(this.X, other.X) &&
        equalfloats(this.Y, other.Y);
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
        this.X = this.X + other.X;
        this.Y = this.Y + other.Y;
    }
    
    public void divide(final float a){
        this.X /= a;
        this.Y /= a;
    }

    @Override
    public void write(final DataOutput out) throws IOException {
        out.writeFloat(this.X);
        out.writeChar(' ');
        out.writeFloat(this.Y);
    }

    @Override
    public void readFields(final DataInput in) throws IOException {
        float x = in.readFloat();
        float y = in.readFloat();

        this.X = x;
        this.Y = y;
    }
}