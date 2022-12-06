package processing.financial.fileprocessor.domain;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;


public class FileSplitter {
    public static List<String> splitTextFiles(String fileName, int maxRows, int ecpectedLineLength) throws IOException, LineLengthException, CreateDirsFailException {
        List<String> newFileList = new ArrayList<>();
        File bigFile = new File(fileName);
        int i = 1;
        String ext = fileName.substring(fileName.lastIndexOf("."));

        String fileNoExt = bigFile.getName().replace(ext, "");
        File newDir = new File(bigFile.getParent() + "//" + fileNoExt + "_split");
        Boolean successFull =  newDir.mkdirs();
        if(successFull.equals(false)){
            throw new CreateDirsFailException("Failed to create directory: " + fileName);
        }
        try (BufferedReader reader = Files.newBufferedReader(Paths.get(fileName)))
        {
            String line = null;
            int lineNum = 1;
            String newSplitFileName = newDir.getPath() + "//" +  fileNoExt + "_" + String.format("%03d", i) + ext;
                    newFileList.add(newSplitFileName);
            Path splitFile = Paths.get(newSplitFileName);
            BufferedWriter writer = Files.newBufferedWriter(splitFile, StandardOpenOption.CREATE);
            while ((line = reader.readLine()) != null)
            {
                int lengthOfLine = line.length();
                if(lengthOfLine!=ecpectedLineLength){
                    StringBuilder error = new StringBuilder("Incorrect line length: ");
                    error.append(fileName);
                    error.append(" line number: ");
                    error.append(lineNum);
                    error.append(" Expected line length:");
                    error.append(ecpectedLineLength);
                    error.append(" Actual line length:");
                    error.append(lengthOfLine);
                    throw new LineLengthException(error.toString());
                }
                writer.append(line);
                writer.newLine();
                lineNum++;
                if (lineNum > maxRows)
                {
                    writer.close();
                    lineNum = 1;
                    i++;
                    newSplitFileName = newDir.getPath() + "//" + fileNoExt + "_" + String.format("%03d", i) + ext;
                    newFileList.add(newSplitFileName);
                    splitFile = Paths.get(newSplitFileName);
                    writer = Files.newBufferedWriter(splitFile, StandardOpenOption.CREATE);
                }
            }

            writer.close();
        }

        System.out.println("file '" + bigFile.getName() + "' split into " + i + " files");
        return newFileList;
    }
}
