package org.elasticflow.model;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.CharBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.EnumSet;

/**
 * read and write big file model
 * @author chengwen
 * @version 1.0
 * @date 2019-01-21 11:15
 * @modify 2019-01-21 11:15
 */
public class MMapFile {

	public void write(byte[] content, String filePath,boolean append) throws Exception { 
        File file = new File(filePath); 
        if(!append)
        	file.delete();
        try (RandomAccessFile randomAccessFile = new RandomAccessFile(file, "rw"))
        { 
            FileChannel fileChannel = randomAccessFile.getChannel(); 
            MappedByteBuffer buffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, file.length(), content.length); 
            buffer.put(content);
        }
	}

	public CharBuffer read(String filePath) throws Exception {
		CharBuffer charBuffer = null;
		Path pathToRead = Paths.get(filePath);
		try (FileChannel fileChannel = (FileChannel) Files.newByteChannel(pathToRead,
				EnumSet.of(StandardOpenOption.READ))) {
			MappedByteBuffer mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, fileChannel.size());
			if (mappedByteBuffer != null) {
				charBuffer = Charset.forName("UTF-8").decode(mappedByteBuffer);
			}
		}
		return charBuffer;
	} 
}
