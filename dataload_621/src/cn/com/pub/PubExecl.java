package cn.com.pub;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

public class PubExecl {
	private static final String EXCEL_XLS = "xls";
	private static final String EXCEL_XLSX = "xlsx";

	public static void main(String[] args) {
		String filePath = "E:\\\\产品与客户场景DEMO\\\\工作分配.xlsx";
		new PubExecl().readDetail(filePath);
	}

	public void readDetail(String filePath) {
		try {
			File excelFile = new File(filePath); // 创建文件对象
			FileInputStream in = new FileInputStream(excelFile); // 文件流
			checkExcelVaild(excelFile);
			Workbook workbook = getWorkbok(in, excelFile);
			int sheetCount = workbook.getNumberOfSheets(); // Sheet的数量
			for (int i = 0; i < sheetCount; i++) {
				Sheet sheet = workbook.getSheetAt(i); 
				String sheetName = sheet.getSheetName();
				int firstRow = sheet.getFirstRowNum();
				int lastRow = sheet.getLastRowNum();
				for(int m=firstRow;m<=lastRow;m++) {
					Row row = sheet.getRow(m);
					if(row==null) {
						continue;
					}
					int firstCol = row.getFirstCellNum();
					int lastCol = row.getLastCellNum();
					for(int n=firstCol;n<lastCol;n++) {
						Cell cell = row.getCell(n);
						System.out.print((sheetName+" "+m+"."+n+":")+(cell!=null?getValue(cell):"")+"|");
					}
					System.out.println();
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	
	public Workbook getWorkbok(InputStream in, File file) throws IOException {
		Workbook wb = null;
		if (file.getName().endsWith(EXCEL_XLS)) { // Excel 2003
			wb = new HSSFWorkbook(in);
		} else if (file.getName().endsWith(EXCEL_XLSX)) { // Excel 2007/2010
			wb = new XSSFWorkbook(in);
		}
		return wb;
	}

	public boolean checkExcelVaild(File file) {
		if (!file.exists()) {
			return false;
		}
		if (!(file.isFile() && (file.getName().endsWith(EXCEL_XLS) || file.getName().endsWith(EXCEL_XLSX)))) {
			return false;
		}
		return true;
	}

	@SuppressWarnings("deprecation")
	public String getValue(Cell cell) {
		if(cell==null) {
			return "";
		}
		Object obj = null;
		switch (cell.getCellTypeEnum()) {
		case BOOLEAN:
			obj = cell.getBooleanCellValue();
			break;
		case ERROR:
			obj = cell.getErrorCellValue();
			break;
		case NUMERIC:
			obj = cell.getNumericCellValue();
			break;
		case STRING:
			obj = cell.getStringCellValue();
			break;
		default:
			break;
		}
		if(obj==null) {
			return "";
		}
		return obj.toString().trim();
	}
}
