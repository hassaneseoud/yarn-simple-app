package org.tcb.yarn;

/**
 * 
 * @author hassane
 *
 */
public class Test {
	private void run(String[] args) {
		
		int begin = Integer.valueOf(args[0]);
		int end = Integer.valueOf(args[1]);
		for (int i=begin;i<end;i++){
			System.out.println("-- "+i);
		}

	}

	public static void main(String[] args) {
		System.out.println("Running Test!");
		if (args.length != 2){
		new Test().run(new String[]{"0","10"});}
		else {
			new Test().run(args);
		}
		System.out.println("Done!");
	}

}
