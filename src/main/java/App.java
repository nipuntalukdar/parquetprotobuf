
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.proto.ProtoParquetReader;
import org.apache.parquet.proto.ProtoParquetWriter;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import com.example.tutorial.AddressBookProtos.AddressBook;
import com.example.tutorial.AddressBookProtos.Person;
import com.example.tutorial.AddressBookProtos.AddressBook.Builder;
import com.example.tutorial.AddressBookProtos.Person.PhoneNumber;
import com.example.tutorial.AddressBookProtos.Person.PhoneType;
import com.google.protobuf.MessageOrBuilder;

import java.util.Random;

public class App {

	public static void main(String[] args) {

		ProtoParquetWriter<AddressBook> prt;
		try {
			prt = new ProtoParquetWriter<AddressBook>(new Path("/tmp/a.prqt"), AddressBook.class,
					CompressionCodecName.GZIP, 1024 * 1024 * 1024, 51290);
			int i = 0;
			Random random = new Random();
			while (i++ < 100000) {
				AddressBook.Builder addressBook = AddressBook.newBuilder();
				Person.Builder pbuilder = Person.newBuilder();
				Person p = pbuilder
						.addPhones(PhoneNumber.newBuilder().setNumber(String.format("%d", random.nextInt(1234567)))
								.setType(PhoneType.HOME))
						.setId(random.nextInt(666666)).setName(String.format("Hello%d", random.nextInt(20)))
						.setEmail(String.format("%d@gamil.com", random.nextInt(9999))).build();

				addressBook.addPeople(p);
				prt.write(addressBook.build());
			}
			prt.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		/// Now read the file
		try {
			ParquetReader<MessageOrBuilder> reader =  ProtoParquetReader
					.<MessageOrBuilder>builder(new Path("/tmp/a.prqt")).build();
			
			Builder messageBuilder = (Builder) reader.read();
			int i = 0, j = 0;
			while (messageBuilder != null) {
				if (i == 0)
					System.out.println(messageBuilder.build());
				i++;
				j++;
				if (i == 1000) {
					i = 0;
				}
				
				messageBuilder = (Builder) reader.read();
			}
			System.out.println(j);

		} catch (IOException e) {
			e.printStackTrace();
		}

	}
}
