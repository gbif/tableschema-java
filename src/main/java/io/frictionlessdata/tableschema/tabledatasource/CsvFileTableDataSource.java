package io.frictionlessdata.tableschema.tabledatasource;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import com.google.common.collect.Iterators;

import io.frictionlessdata.tableschema.exception.TableSchemaException;

public class CsvFileTableDataSource extends AbstractTableDataSource<String> {

    CsvFileTableDataSource(String sourceFilePath) {
        this.dataSource = sourceFilePath;
    }

    private CSVFormat format = TableDataSource.getDefaultCsvFormat();

    public void setFormat(CSVFormat format) {
        this.format = format;
    }

    public CSVFormat getFormat() {
        return (this.format != null)
                ? this.format
                : TableDataSource.getDefaultCsvFormat();
    }

    @Override
    public Iterator<String[]> iterator() {
        CSVParser parser;
        try {
            parser = getCSVParser();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        Iterator<CSVRecord> iterCSVRecords = parser.iterator();

        return Iterators.transform(iterCSVRecords, (CSVRecord input) -> {
            Iterator<String> iterCols = input.iterator();

            List<String> cols = new ArrayList<>();
            while (iterCols.hasNext()) {
                cols.add(iterCols.next());
            }

            return cols.toArray(new String[0]);
        });
    }

    @Override
    public String[] getHeaders() {
        if (null == headers) {
            // Get a copy of the header map that iterates in column order.
            // The map keys are column names. The map values are 0-based indices.
            Map<String, Integer> headerMap = null;
            try {
                headerMap = getCSVParser().getHeaderMap();
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
            if (null == headerMap) {
                return null;
            }
            headers = headerMap.keySet().toArray(new String[0]);
        }
        return headers;
    }

    private CSVParser getCSVParser() throws IOException {
        CSVFormat format = getFormat();

        if (null != dataSource) {
            return CSVParser.parse(new File(dataSource), StandardCharsets.UTF_8, format);
        } else {
            throw new TableSchemaException("Data source is of invalid type.");
        }
    }

    @Override
    public boolean hasReliableHeaders() {
        try {
            return this.getHeaders() != null;
        } catch (Exception ex) {
            return false;
        }
    }
}