package ghtk.masterdev.kafka;

import com.opencsv.bean.CsvBindByName;

public class Customer {
    @CsvBindByName(column = "id")
    private int id;

    @CsvBindByName(column = "num_order")
    private int num_order;

    @CsvBindByName(column = "age")
    private int age;

    @CsvBindByName(column = "tel")
    private String tel;

    public Customer() {
    }

    public Customer(int id, int num_order, int age, String tel) {
        this.id = id;
        this.num_order = num_order;
        this.age = age;
        this.tel = tel;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public int getNum_order() {
        return num_order;
    }

    public void setNum_order(int num_order) {
        this.num_order = num_order;
    }

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }

    public String getTel() {
        return tel;
    }

    public void setTel(String tel) {
        this.tel = tel;
    }
}
