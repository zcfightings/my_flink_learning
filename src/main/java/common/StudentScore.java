package common;

public class StudentScore {
    public int id;
    public int cla;
    public int chinese_score;
    public int math_score;
    public int english_score;
    public long ts;

    public StudentScore () {}

    public StudentScore(int id, int cla, int chinese_score, int math_score, int english_score, long ts) {
        this.id = id;
        this.cla = cla;
        this.chinese_score = chinese_score;
        this.math_score = math_score;
        this.english_score = english_score;
        this.ts = ts;
    }

    public String toString() {
        return "id:" + id + ", cla:" + cla + ", chinese_score:" + chinese_score + ", math_score:" + math_score + ", english_score:" + english_score + ", ts:" + ts;
    }
}
