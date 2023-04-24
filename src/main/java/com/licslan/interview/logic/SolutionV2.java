package com.licslan.interview.logic;

import org.json.JSONException;
import org.json.JSONObject;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.*;
import java.util.concurrent.*;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

/**
 * @author LICSLAN - WEILIN
 * 需求：
 * 有1000 个 20M 大小的文本文件,文件中每行数据的格式固定为: {"name": "xx","timestamp": xx, "content": "xx"},
 * 已经下载电脑D盘 D:\test-read-file\records.zip,解压后里面的文件夹records里面的文件格式按照
 * record_0.txt ...  record_999.txt 文件共计1000个.  name: 字符串, 长度为 32 个字节以内,timestamp: 毫秒级时间戳,
 * content: 字符串,长度为 1024 个字节以内.   规则 : a. 找出所有 name 为 zhangsan [张三] 的行,并按照 timestamp 从小到大排序,
 * 将排序好的结果重新写入新的文件,规则如下:按照 timestamp 以天为维度组织文件,新文件的命名规则为 年-月-日,例如 (2022-1-12),将
 * 所有位于同一天的数据存放于同一个文件中.
 * <p>
 * 实验机器
 * 作者机器 window7  16g  4c  JDK8
 * 注意如果是其他系统 比如: Linux 需要代码中调整目录格式
 * 请先提前下载文件到本机  https://mc-public-resource-cn.s3.cn-north-1.amazonaws.com.cn/records.zip
 * 下载后放在  D:\test-read-file
 *
 * java -Xms2g -Xmx14g -Xmn4g -jar xxx.jar
 *
 * 耗时时间 解压时间耗时最久  相关测试数据对比  耗时时间单位 秒
 *
 *
 *
 *
 * byte[] buffer = new byte[4096];
 * unZipTime =========>188.726730938
 * size and count is ====905---905
 * matchFileTime =========>1.002636965
 * extractedFileToPathTime =========>0.216706957
 * 总体耗时 =========>189.946125864
 *
 * byte[] buffer = new byte[8192];
 * unZipTime =========>172.700023543
 * size and count is ====905---905
 * matchFileTime =========>29.276718047
 * extractedFileToPathTime =========>0.484222838
 * 总体耗时 =========>202.46102725
 * 解题思路: 1.下载解压文件  2.遍历每个解压后的文件  3.拿着关键字去匹配  4.将同一天分组以年月日为key  5.写入新的文件
 * byte[] buffer = new byte[8192];
 * unZipTime =========>178.322301869
 * size and count is ====905---905
 * matchFileTime =========>0.94229466
 * extractedFileToPathTime =========>0.23293429
 * 总体耗时 =========>179.497697515
 *
 *  byte[] buffer = new byte[8192*2];
 * unZipTime =========>182.898074571
 * size and count is ====905---905
 * matchFileTime =========>6.159314171
 * extractedFileToPathTime =========>10.312937363
 * 总体耗时 =========>199.370393592
 * 解题思路: 1.下载解压文件  2.遍历每个解压后的文件  3.拿着关键字去匹配  4.将同一天分组以年月日为key  5.写入新的文件
 *
 *  测试结果因不同机器配置而已 JVM 配置情况不同而不同 以及可以使用解压算法更好的第三方程序决定总耗时  Java 解压算法并非最佳
 * 对比python 运行时间  python时间上更久
 *
 **/


public class SolutionV2 {

    /**
     * Compressed file drive letter
     */
    public static final String ZIP_FILE_PATH = "D:\\test-read-file\\records.zip";

    /**
     * Directory for file decompression
     */
    public static final String UNZIP_FILE_PATH = "D:\\test-read-file\\";

    /**
     * Directory for storing files after decompression
     */
    final static String INPUT_FOLDER_PATH = "D:\\test-read-file\\records\\";

    /**
     * Directory for generating new files after processing according to conditions
     */
    final static String OUTPUT_FOLDER_PATH = "D:\\test-read-file\\output\\";

    /**
     * Keyword match FIXME 注意 面试题中给的 zhangsan 经过解压后未发现  跑程序后也没有  发现是汉字张三  也许面试官故意考察的 ^_^ do u?
     */
    final static String KEYWORD_MATCH = "张三";

    public static void main(String[] args) throws IOException {

        long startTime = System.nanoTime();
        //1.unzip
        if (!checkNum()) {
            unzipV2();
        }
        long unZipTime = System.nanoTime();
        System.out.println("unZipTime =========>" + (unZipTime - startTime) / 1000000000.0);

        //2. Create a Map to store the lines with timestamp as key Iterate through each text file in the records folder
        Map<Long, List<String>> timestampMap = getLongListMap();
        long matchFileTime = System.nanoTime();
        System.out.println("matchFileTime =========>" + (matchFileTime - unZipTime) / 1000000000.0);
        if (timestampMap == null) return;

        //3. Iterate through the sorted Map and write each line to a new file with a name based on the timestamp
        extractedFileToPath(timestampMap);
        long extractedFileToPathTime = System.nanoTime();
        System.out.println("extractedFileToPathTime =========>" + (extractedFileToPathTime - matchFileTime) / 1000000000.0);

        //4. Time consumption statistics
        long endTime = System.nanoTime();
        double time = (endTime - startTime) / 1000000000.0;
        System.out.println("总体耗时 =========>" + time);

        System.out.println("解题思路: 1.下载解压文件  2.遍历每个解压后的文件  3.拿着关键字去匹配  4.将同一天分组以年月日为key  5.写入新的文件");

    }

    private static void extractedFileToPath(Map<Long, List<String>> timestampMap) throws IOException {
        File destDir = new File(OUTPUT_FOLDER_PATH);
        if (!destDir.exists()) {
            destDir.mkdirs();
        }

        //Put them together on the same day
        Map<String, List<String>> newTimestampMap = new TreeMap<>();
        for (Map.Entry<Long, List<String>> entry : timestampMap.entrySet()) {
            Long timestamp = entry.getKey();
            List<String> values = entry.getValue();
            Instant instant = Instant.ofEpochMilli(timestamp);
            LocalDate date = instant.atZone(ZoneId.systemDefault()).toLocalDate();
            String newKey = date.toString();
            newTimestampMap.computeIfAbsent(newKey, k -> new ArrayList<>()).addAll(values);
        }
        for (Map.Entry<String, List<String>> entry : newTimestampMap.entrySet()) {
            String dateTime = entry.getKey();
            //LocalDateTime dateTime = LocalDateTime.ofInstant(Instant.ofEpochMilli(timestamp), ZoneId.systemDefault());
            String outputFilePath = OUTPUT_FOLDER_PATH + "/" + dateTime + ".txt";
            BufferedWriter writer = Files.newBufferedWriter(Paths.get(outputFilePath), StandardCharsets.UTF_8);
            List<String> lines = entry.getValue();
            // Sort the lines by timestamp in ascending order
            lines.sort((line1, line2) -> {
                try {
                    long timestamp1 = new JSONObject(line1).getLong("timestamp");
                    long timestamp2 = new JSONObject(line2).getLong("timestamp");
                    return Long.compare(timestamp1, timestamp2);
                } catch (JSONException e) {
                    // Ignore the line if it's not a valid JSON object
                    return 0;
                }
            });
            // Write each line to the output file
            for (String line : lines) {
                writer.write(line);
                writer.newLine();
            }
            writer.close();
        }
    }


    private static Map<Long, List<String>> getLongListMap() throws IOException {
        Map<Long, List<String>> timestampMap = new TreeMap<>();
        //调试用
        int count = 0;
        for (int i = 0; i < 1; i++) {
            String filePath = INPUT_FOLDER_PATH + "record_" + i + ".txt";
            BufferedReader reader = new BufferedReader(new InputStreamReader(Files.newInputStream(Paths.get(filePath)), StandardCharsets.UTF_8));
            String line;
            // Read each line to extract the name and timestamp values
            while ((line = reader.readLine()) != null) {
                try {
                    // Parse the line as a JSON object to extract the name and timestamp values
                    JSONObject jsonObject = new JSONObject(line);
                    String name = jsonObject.getString("name");
                    //System.out.println("name============>"+name);
                    long timestamp = jsonObject.getLong("timestamp");
                    // If the name is "zhangsan" 张三, store the line in the Map with the timestamp as the key
                    if (KEYWORD_MATCH.equals(name)) {
                        //调试所用
                        count++;
                        timestampMap.computeIfAbsent(timestamp, k -> new ArrayList<>()).add(line);
                    }
                } catch (JSONException e) {
                    // Ignore the line if it's not a valid JSON object
                }
            }
            reader.close();
        }
        System.out.println("size and count is ====" + timestampMap.size() + "---" + count);
        if (timestampMap.isEmpty()) {
            return null;
        }
        return timestampMap;
    }


    private static boolean checkNum() {
        // 替换成你要检查的文件夹路径
        File folder = new File(INPUT_FOLDER_PATH);
        if (folder.isDirectory()) {
            File[] files = folder.listFiles();
            //算上.DS_Store 一共1001个  当然这里可以做优化 根据实际情况不用硬编码1001
            return files != null && files.length == 1001;
        } else {
            System.out.println("该路径不是文件夹,目前还没解压,正在解压中~~~");
            return false;
        }
    }




    /**
     * 需要注意的是，使用多线程并不一定总能提高解压缩效率。具体效果取决于我们的硬件配置、
     * 文件大小、线程数等多个因素。我们需要根据具体情况进行调整和优化不断测试找出比较合理的方案
     * 经过测试发现效果不太明显  期间会打开浏览器  操作其他的程序  导致优化结果不太明显  需要保证运行环境一致 再进行测试
     * */
    private static void unzipV2(){

        try (ZipFile zipFile = new ZipFile(ZIP_FILE_PATH)) {
            Enumeration<? extends ZipEntry> entries = zipFile.entries();

            // 获取当前机器可用的处理器核心数
            int processors = Runtime.getRuntime().availableProcessors();
            System.out.println("当前机器可用的处理器核心数为：" + processors);

            // 创建固定大小的线程池
            ExecutorService executorService = Executors.newFixedThreadPool(processors);

            // 存储解压缩任务的Future对象
            List<Future<?>> futures = new ArrayList<>();

            while (entries.hasMoreElements()) {
                ZipEntry entry = entries.nextElement();
                if (!entry.isDirectory()) {
                    String entryName = entry.getName();

                    // 创建解压后的文件
                    Path filePath = Paths.get(UNZIP_FILE_PATH, entryName);
                    if (!Files.exists(filePath.getParent())) {
                        Files.createDirectories(filePath.getParent());
                    }
                    Files.createFile(filePath);

                    // 创建解压缩任务
                    Callable<Void> task = () -> {
                        try (InputStream inputStream = zipFile.getInputStream(entry);
                             OutputStream outputStream = Files.newOutputStream(filePath);
                             BufferedInputStream bufferedInputStream = new BufferedInputStream(inputStream);
                             BufferedOutputStream bufferedOutputStream = new BufferedOutputStream(outputStream)) {
                            byte[] buffer = new byte[1024 * 4];
                            int bytesRead;
                            while ((bytesRead = bufferedInputStream.read(buffer)) != -1) {
                                bufferedOutputStream.write(buffer, 0, bytesRead);
                            }
                        }
                        return null;
                    };

                    // 将解压缩任务提交到线程池，并将Future对象添加到futures集合中
                    Future<?> future = executorService.submit(task);
                    futures.add(future);
                }
            }

            // 等待所有解压缩任务完成
            for (Future<?> future : futures) {
                try {
                    future.get();
                } catch (ExecutionException e) {
                    throw new RuntimeException(e);
                }
            }

            // 关闭线程池
            executorService.shutdown();
        } catch (IOException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }


}
