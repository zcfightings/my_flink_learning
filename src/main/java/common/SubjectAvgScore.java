package common;

public class SubjectAvgScore {
    public String subject;
    public int id;
    public double avg_score;

    public SubjectAvgScore() {}

    public SubjectAvgScore(String subject, int id, double avg_score)
    {
        this.subject = subject;
        this.id = id;
        this.avg_score = avg_score;
    }

    @Override
    public String toString() {
        return "subject:" + subject + ", id:" + id + ", avg_score:" + avg_score;
    }
}
