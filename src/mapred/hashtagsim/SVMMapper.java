package mapred.hashtagsim;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import libsvm.svm;
import libsvm.svm_model;
import libsvm.svm_node;
import libsvm.svm_parameter;
import libsvm.svm_problem;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SVMMapper extends Mapper<LongWritable, Text, LongWritable, Text>{

	private List<Double> yLabels = new ArrayList<Double>();
	private List<List<Double>> trainData = new ArrayList<List<Double>>();
	
	private double[] y;
	private svm_node[][] x;
	
	private static final int DIVIDE_NUM = 100;
	
	private int globalCount = 0;
	private static final int PER_MAPPER_NUM = 1000;
	
	
	
	protected svm_model trainModel(svm_node[][] trainData, double[] trainLabel){
		//定义svm_problem对象
		svm_problem problem = new svm_problem();
		problem.l = trainData.length; //向sses量个数
		problem.x = trainData; //训练集向量表
		problem.y = trainLabel; //对应的lable数组
		
		svm_parameter param = new svm_parameter();
		param.svm_type = svm_parameter.C_SVC;
		param.kernel_type = svm_parameter.LINEAR;
		param.cache_size = 200;
		param.eps = 0.001;
		param.C = 1;
		
		svm.svm_check_parameter(problem, param);
		if(problem.x == null || problem.y == null){
			return null;
		}
		try{
			svm_model model = svm.svm_train(problem, param);
			return model;
		}catch(Exception e){
			//do nothing.
			e.printStackTrace();
			System.out.println("-----failed--------");
		}	
		
		return null;
	}
	
	
	protected void convertData(){
		Double[] yTemp = yLabels.toArray(new Double[yLabels.size()]);
		y = new double[yLabels.size()];
		for(int i = 0; i < yTemp.length; ++i){
			y[i] = yTemp[i];
		}
		x = new svm_node[trainData.size()][];
		for(int i = 0; i < trainData.size(); ++i){
			svm_node[] temp = new svm_node[trainData.get(i).size()];
			for(int j = 0; j < trainData.get(i).size(); ++j){
				svm_node node = new svm_node();
				node.index = j;
				node.value = trainData.get(i).get(j);
				temp[j] = node;
			}
			x[i] = temp;
		}
	}

	protected int[] getSupportVectorLabels(svm_node[][] supportVectors, svm_model model){
		int[] labels = new int[supportVectors.length];
		for(int i = 0; i < supportVectors.length; ++i){
			labels[i] = (int)svm.svm_predict(model, supportVectors[i]);
		}
		return labels;
	}
	
	protected void outputSupportVectors(int key, Context context, svm_node[][] supportVectors, int[] labels) throws IOException, InterruptedException{
		StringBuilder sb = new StringBuilder();
		for(int i = 0; i < supportVectors.length; ++i){
			sb.setLength(0);
			sb.append(labels[i]);
			sb.append(":");
			for(int j = 0;j < supportVectors[i].length; ++j){
				sb.append(supportVectors[i][j].value);
				if(j != supportVectors[i].length - 1)
					sb.append(":");
			}
			context.write(new LongWritable(key), new Text(sb.toString()));
		}
	}

	protected void outputDataSet(int key, Context context, svm_node[][] supportVectors, double[] labels) throws IOException, InterruptedException{
		StringBuilder sb = new StringBuilder();
		for(int i = 0; i < supportVectors.length; ++i){
			sb.setLength(0);
			sb.append((int)labels[i]);
			sb.append(":");
			for(int j = 0;j < supportVectors[i].length; ++j){
				sb.append(supportVectors[i][j].value);
				if(j != supportVectors[i].length - 1)
					sb.append(":");
			}
			context.write(new LongWritable(key), new Text(sb.toString()));
		}
	}
	@Override
	protected void cleanup(Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		
		if(globalCount < PER_MAPPER_NUM && globalCount > 0){
			execute(context);
			yLabels.clear();
			trainData.clear();
			globalCount = 0;
		}
	}

	protected void execute(Context context) throws IOException, InterruptedException{
		convertData();
		Random random = new Random();
		int key = random.nextInt() % DIVIDE_NUM;
		svm_model model = trainModel(x, y);
		svm_node[][] supportVectors = model.SV;
		//if we cannot find any support vectors, we just out put the whole dataset
		if(model.SV == null || model.SV.length == 0){
			outputDataSet(key, context, x, y);
		}else{
			int[] labels = getSupportVectorLabels(supportVectors, model);
			outputSupportVectors(key, context, supportVectors, labels);
		}
	}

	@Override
	protected void map(LongWritable key, Text value,
			Context context)
			throws IOException, InterruptedException {
		++globalCount;
		if(globalCount < PER_MAPPER_NUM){
			String line = value.toString();
			String[] tokens = line.split("\\s+");
			yLabels.add(Double.valueOf(Double.parseDouble(tokens[0]) >= 1? 1: 0));
			List<Double> data = new ArrayList<Double>();
			for(int i = 1; i < tokens.length; ++i){
				if(tokens[i].length() > 10)
					continue;
				data.add(Double.parseDouble(tokens[i]));
			}
			trainData.add(data);
		}else{
			execute(context);
			yLabels.clear();
			trainData.clear();
			globalCount = 0;
		}
	}

	@Override
	protected void setup(Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
	}
	
}
