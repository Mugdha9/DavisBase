package OutputHandler;

public class Output {
    int Affectedrows;
    private boolean isExecuted = false;
    public Output(int Affectedrows) {
        this.Affectedrows = Affectedrows;
    }

    public Output(int Affectedrows,boolean isExecuted) {
        this.Affectedrows = Affectedrows;
        this.isExecuted = isExecuted;
    }

    public void Display() {
        if(this.isExecuted) return;
        System.out.println("Query Successful");
        if(this.Affectedrows == 1)
            System.out.println(String.format("%d row affected", this.Affectedrows));
        else
            System.out.println(String.format("%d rows affected", this.Affectedrows));
        System.out.println();
    }
}
