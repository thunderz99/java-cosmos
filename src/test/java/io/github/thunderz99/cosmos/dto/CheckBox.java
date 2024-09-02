package io.github.thunderz99.cosmos.dto;

/**
 * A test pojo class to be used in patch
 */
public class CheckBox extends RecordData {

    public enum Align {
        HORIZONTAL, VERTICAL
    }

    public String id;
    public String name;
    public Align align = Align.HORIZONTAL;

    public CheckBox(){
    }

    public CheckBox(String id, String name, Align align){
        this.id = id;
        this.name = name;
        this.align = align;
    }

}
