using Halforbit.DataStores.FileStores.Interface;
using SevenZip.Compression.LZMA;
using System;
using System.IO;
using System.Threading.Tasks;

namespace Halforbit.DataStores.FileStores.Compression.GZip.Implementation
{
    public class LzmaCompressor : ICompressor
    {
        public Task<byte[]> Compress(byte[] value)
        {
            using (var sourceStream = new MemoryStream(value))
            using (var destStream = new MemoryStream())
            {
                var coder = new Encoder();

                // Write the encoder properties

                coder.WriteCoderProperties(destStream);

                // Write the decompressed file size.

                destStream.Write(BitConverter.GetBytes(sourceStream.Length), 0, 8);

                // Encode the file.

                coder.Code(sourceStream, destStream, sourceStream.Length, -1, null);

                destStream.Flush();
                
                return Task.FromResult(destStream.ToArray());
            }
        }

        public Task<byte[]> Decompress(byte[] data)
        {
            using (var sourceStream = new MemoryStream(data))
            using (var destStream = new MemoryStream())
            {
                var coder = new Decoder();

                // Read the decoder properties

                var properties = new byte[5];

                sourceStream.Read(properties, 0, 5);

                // Read in the decompressed file size.

                var fileLengthBytes = new byte[8];

                sourceStream.Read(fileLengthBytes, 0, 8);

                var fileLength = BitConverter.ToInt64(fileLengthBytes, 0);

                coder.SetDecoderProperties(properties);

                coder.Code(sourceStream, destStream, sourceStream.Length, fileLength, null);

                destStream.Flush();

                return Task.FromResult(destStream.ToArray());
            }
        }
    }
}
