package io.github.thunderz99.cosmos.dto;

import io.github.thunderz99.cosmos.util.JsonUtil;

import java.util.ArrayList;
import java.util.List;

public class FullNameUser {
    public String id;

    public FullName fullName;

    public int age;

    public String mail;
    /**
     * a reserved word in cosmosdb ( to test c["end"])
     */
    public String end;

    public List<String> skills = new ArrayList<>();

    public FullNameUser() {
    }

    public FullNameUser(String id, String firstName, String lastName, int age, String mail, String end, String... skills) {
        this.id = id;
        this.fullName = new FullName(firstName, lastName);
        this.age = age;
        this.mail = mail;
        this.end = end;
        if (skills != null) {
            this.skills.addAll(List.of(skills));
        }
    }

    @Override
    public String toString() {
        return JsonUtil.toJson(this);
    }

    public static class FullName {
        public String first;
        public String last;

        public FullName() {
        }

        public FullName(String first, String last) {
            this.first = first;
            this.last = last;
        }

        @Override
        public String toString() {
            return JsonUtil.toJson(this);
        }
    }

    public enum Skill {
        Typescript, Javascript, Java, Python, Go
    }
}


