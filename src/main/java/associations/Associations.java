package associations;

public class Associations {
    private static String outPath;
    private static int fileCount;
    public enum Tag {
        Lexeme,
        Feature,
        Count,
    }
    public static void main(String[] args) {
        if (args.length != 3) {
            System.err.println("Expected <input-corpus-path> <output-path> <corpus-file-count>");
            System.exit(1);
        }
        String inputPath = args[0];
        outPath = args[1];
        fileCount = Integer.parseInt(args[2]);

    }
}
