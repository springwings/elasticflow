package org.elasticflow.computer.algorithm;

import java.awt.Graphics2D;
import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;

import javax.imageio.ImageIO;

import org.elasticflow.util.EFException;
import org.elasticflow.util.EFException.ELEVEL;

public class ImageUtil {

	public static BufferedImage readImage(String imageFile) {
		File file = new File(imageFile);
		BufferedImage bf = null;
		try {
			bf = ImageIO.read(file);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return bf;
	}

	public static BufferedImage resize(BufferedImage sourceImg,int newWidth,int newHeight) throws IOException {
//        double scale = (newWidth/sourceImg.getWidth() < newHeight/sourceImg.getHeight())?newWidth/sourceImg.getWidth():newHeight/sourceImg.getHeight();
//    	int _width = (int) (scale * sourceImg.getWidth());
//        int _height = (int) (scale * sourceImg.getHeight()); 
        
        BufferedImage newImg = new BufferedImage(newWidth, newHeight, BufferedImage.TYPE_INT_RGB);
        Graphics2D graphics2D = newImg.createGraphics();  
        graphics2D.drawImage(sourceImg, 0, 0, newWidth, newHeight, null);
        graphics2D.dispose();
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        System.out.println(baos.toByteArray()[0]);
        InputStream in = new ByteArrayInputStream(baos.toByteArray());	
        return ImageIO.read(in);        
	}

	public static int[][][][] getImgArrays(String[] imageFile,int newWidth,int newHeight) throws EFException  {
		BufferedImage[] bf = new BufferedImage[imageFile.length];
		for (int i = 0; i < imageFile.length; i++) {
			try {
				bf[i] = resize(readImage(imageFile[i]),newWidth,newHeight);
			} catch (IOException e) {
				throw new EFException(e,"read image exception",ELEVEL.Dispose);
			}
		}
		return convertTO4DArray(bf);
	}

	public static int[][][][] convertTO4DArray(BufferedImage[] bf) {
		int width = bf[0].getWidth();
		int height = bf[0].getHeight();
		int channel = 3;
		int r = 0;
		int g = 1;
		int b = 2;		
		int[][][][] rgb4DArray = new int[bf.length][width][height][channel];
		for (int idx=0;idx<bf.length;idx++) {
			int[] data = new int[width * height];
			bf[idx].getRGB(0, 0, width, height, data, 0, width);
			for (int i = 0; i < height; i++) {				
				for (int j = 0; j < width; j++) {
					rgb4DArray[idx][j][i][r] = (data[i * width + j] & 0xff0000) >> 16;
					rgb4DArray[idx][j][i][g] = (data[i * width + j] & 0xff00) >> 8;
					rgb4DArray[idx][j][i][b] = (data[i * width + j] & 0xff);
				}
			}
		}		
		return rgb4DArray;
	}
}
