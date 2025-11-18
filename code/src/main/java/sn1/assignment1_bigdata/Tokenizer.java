package sn1.assignment1_bigdata;

import java.util.ArrayList;
import java.util.List;

public class Tokenizer {

    public static List<String> tokenize(String line) {
        List<String> tokens = new ArrayList<>();
        if (line == null) {
            return tokens;
        }

        String[] parts = line.toLowerCase()
                             .replaceAll("[^a-z0-9\\s]", " ")
                             .split("\\s+");

        for (String p : parts) {
            if (!p.isEmpty()) {
                tokens.add(p);
            }
        }
        return tokens;
    }
}
