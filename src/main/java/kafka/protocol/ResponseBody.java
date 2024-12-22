package kafka.protocol;

import kafka.protocol.io.DataOutput;

public interface ResponseBody {

	void serialize(DataOutput output);

}