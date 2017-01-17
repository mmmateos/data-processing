package util;

import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

// See:
//  http://codereview.stackexchange.com/questions/62500/split-camel-cased-snake-cased-string
//  http://stackoverflow.com/questions/2559759/how-do-i-convert-camelcase-into-human-readable-names-in-java
public class Tokenization {
	private static final Pattern PUNCTSPACE = Pattern.compile("[ \\p{Punct}]+");
	private static final Pattern TRANSITION = Pattern.compile(
			String.format("%s|%s|%s",
					"(?<=[\\p{javaUpperCase}])(?=[\\p{javaUpperCase}][\\p{javaLowerCase}])",
					"(?<=[^\\p{javaUpperCase}])(?=[\\p{javaUpperCase}])",
					"(?<=[\\p{javaLowerCase}\\p{javaUpperCase}}])(?=[^\\p{javaLowerCase}\\p{javaUpperCase}])"
			)
	);

	public static List<String> split(String text) {
		List<String> result = new ArrayList<>();
		for (String word : PUNCTSPACE.split(text)) {
			if (word.isEmpty()) {
				continue;
			}
			Collections.addAll(result, TRANSITION.split(word));
		}
		return result.isEmpty() ? Collections.singletonList(text) : result;
	}

	public static Map<String, Boolean> contains(String name, List<String> keywords) {
		List<String> tokens = Tokenization.split(name);
		tokens = tokens.stream().map(String::toLowerCase).collect(Collectors.toList());
		Map<String, Boolean> result = new HashMap<>();

		for (String keyword : keywords) {
			if (keyword.equals(tokens.get(0)))
				result.put(keyword, true);
			else if (keyword.equals(tokens.get(tokens.size() - 1)))
				result.put(keyword, true);
			else if (tokens.size() > 1 && keyword.equals(tokens.get(tokens.size() - 2)) && tokens.get(tokens.size() - 1).matches("[0-9]+"))
				result.put(keyword, true);
			else
				result.put(keyword, false);
		}

		return result;
	}
}
