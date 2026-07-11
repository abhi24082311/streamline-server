const express = require('express')
const { Server } = require('socket.io')
const cors = require('cors');
const http = require('http')
const app = express();
const fs = require('fs')
const path = require('path')
const { Readable } = require('stream')
const axios = require('axios')
const { S3Client, PutObjectCommand } = require('@aws-sdk/client-s3')
const dotenv = require('dotenv')
const Groq = require('groq-sdk')
const ffmpeg = require('fluent-ffmpeg')
const ffmpegInstaller = require('@ffmpeg-installer/ffmpeg')
ffmpeg.setFfmpegPath(ffmpegInstaller.path)
dotenv.config()

const groq = new Groq({
    apiKey: process.env.GROQ_API_KEY,
})

const s3 = new S3Client({
    credentials: {
        accessKeyId: process.env.ACCESS_KEY,
        secretAccessKey: process.env.SECRET_KEY,
    },
    region: process.env.BUCKET_REGION
})

const server = http.createServer(app);
console.log(process.env.ELECTRON_HOST)

/**
 * Re-mux a raw MediaRecorder WebM file so it has a proper Cues (seek index).
 * Without this, browsers cannot seek in the video — the slider resets to 0.
 * FFmpeg -c copy is lossless: it only rebuilds the container, not re-encodes.
 */
function remuxWebm(inputPath, outputPath) {
    return new Promise((resolve, reject) => {
        ffmpeg(inputPath)
            .outputOptions([
                '-c copy',           // lossless: copy streams as-is
                '-movflags +faststart' // move index to front (helps streaming)
            ])
            .output(outputPath)
            .on('end', () => {
                console.log('🎬 Re-mux complete:', outputPath)
                resolve()
            })
            .on('error', (err) => {
                console.error('❌ FFmpeg remux error:', err.message)
                reject(err)
            })
            .run()
    })
}

/**
 * Extract a thumbnail frame from the video at a given timestamp.
 * Uses FFmpeg to seek to `timeOffset` seconds and grab 1 frame as JPEG.
 */
function extractThumbnail(inputPath, outputPath, timeOffset = 1) {
    return new Promise((resolve, reject) => {
        ffmpeg(inputPath)
            .seekInput(timeOffset)
            .frames(1)
            .outputOptions([
                '-q:v 2',        // high quality JPEG (scale 2-31, lower = better)
                '-vf scale=640:-2' // 640px width, auto height (even number)
            ])
            .output(outputPath)
            .on('end', () => {
                console.log('🖼️ Thumbnail extracted:', outputPath)
                resolve()
            })
            .on('error', (err) => {
                console.error('❌ FFmpeg thumbnail error:', err.message)
                reject(err)
            })
            .run()
    })
}

app.use(cors())

const io = new Server(server, {
    cors: {
        origin: process.env.ELECTRON_HOST,
        methods: ['GET', 'POST'],
    },
})

io.on('connection', (socket) => {
    console.log('🟢 Socket is connected')

    socket.on('video-chunks', async (data) => {
        console.log('🟢 Video chunk received')
        try {
            // ArrayBuffer arrives from client — convert to Node Buffer
            const chunk = Buffer.isBuffer(data.chunks)
                ? data.chunks
                : Buffer.from(data.chunks)
            fs.appendFileSync('temp_upload/' + data.filename, chunk)
            console.log('🟢 Chunk saved')
        } catch (err) {
            console.error('❌ Error saving chunk:', err)
        }
    })

    socket.on('process-video', async (data) => {
        console.log('🟢 Processing video:', data.filename)

        const rawPath = path.join('temp_upload', data.filename)
        const fixedFilename = data.filename.replace(/\.webm$/, '_fixed.webm')
        const fixedPath = path.join('temp_upload', fixedFilename)

        // Thumbnail paths
        const thumbnailFilename = data.filename.replace(/\.webm$/, '_thumb.jpg')
        const thumbnailPath = path.join('temp_upload', thumbnailFilename)

        // 1. Mark video as processing in the DB (creates the video record)
        let processing;
        try {
            processing = await axios.post(
                `${process.env.NEXT_API_HOST}recording/${data.userId}/processing`,
                { filename: data.filename }
            )
            if (processing.data.status !== 200) {
                return console.log('❌ Failed to process:', processing.data)
            }
        } catch (error) {
            console.error('❌ Axios error in processing request:', error.response?.data || error.message)
            return
        }

        // 2. Re-mux the raw MediaRecorder WebM to add a proper seek index (Cues)
        try {
            console.log('🎬 Re-muxing WebM to fix seekability...')
            await remuxWebm(rawPath, fixedPath)
        } catch (err) {
            console.error('❌ Remux failed, uploading original as fallback:', err.message)
            fs.copyFileSync(rawPath, fixedPath)
        }

        // 3. Extract thumbnail from the fixed video
        try {
            console.log('🖼️ Extracting thumbnail...')
            await extractThumbnail(fixedPath, thumbnailPath, 1)

            // Upload thumbnail to S3
            const thumbBuffer = fs.readFileSync(thumbnailPath)
            const thumbCommand = new PutObjectCommand({
                Key: thumbnailFilename,
                Bucket: process.env.BUCKET_NAME,
                ContentType: 'image/jpeg',
                Body: thumbBuffer,
            })
            const thumbStatus = await s3.send(thumbCommand)

            if (thumbStatus['$metadata'].httpStatusCode === 200) {
                console.log('🟢 Thumbnail uploaded to S3:', thumbnailFilename)

                // Save thumbnail filename in the DB
                try {
                    await axios.post(
                        `${process.env.NEXT_API_HOST}recording/${data.userId}/thumbnail`,
                        { filename: data.filename, thumbnail: thumbnailFilename }
                    )
                    console.log('🟢 Thumbnail saved to DB')
                } catch (dbErr) {
                    console.error('❌ Failed to save thumbnail to DB:', dbErr.message)
                }
            } else {
                console.log('❌ Thumbnail S3 upload failed')
            }

            // Clean up thumbnail temp file
            fs.unlink(thumbnailPath, () => {})
        } catch (err) {
            console.error('❌ Thumbnail extraction failed (non-blocking):', err.message)
        }

        // 4. Upload the fixed (seekable) video file to S3
        const fileToUpload = fs.readFileSync(fixedPath)
        const command = new PutObjectCommand({
            Key: data.filename,
            Bucket: process.env.BUCKET_NAME,
            ContentType: 'video/webm',
            Body: fileToUpload,
        })

        const fileStatus = await s3.send(command)

        if (fileStatus['$metadata'].httpStatusCode === 200) {
            console.log('🟢 Video uploaded to S3 (seekable)')

            // 5. Transcribe + generate title/summary for ALL users (no plan gate)
            try {
                const stat = fs.statSync(fixedPath)

                // Groq Whisper free tier has a 25MB file limit
                if (stat.size < 25000000) {
                    console.log('🎙️ Starting Groq Whisper transcription...')
                    const transcription = await groq.audio.transcriptions.create({
                        file: fs.createReadStream(fixedPath),
                        model: 'whisper-large-v3',
                        response_format: 'text',
                    })

                    if (transcription) {
                        console.log('✅ Transcription done, generating title/summary via Groq Llama...')
                        const completion = await groq.chat.completions.create({
                            model: 'llama-3.3-70b-versatile',
                            response_format: { type: 'json_object' },
                            messages: [
                                {
                                    role: 'system',
                                    content: `You are going to generate a title and a nice description using the speech to text transcription provided: transcription(${transcription}) and then return it in json format as {"title": <the title you gave>, "summary": <the summary you created>}`,
                                },
                            ],
                        })

                        const titleAndSummaryGenerated = await axios.post(
                            `${process.env.NEXT_API_HOST}recording/${data.userId}/transcribe`,
                            {
                                filename: data.filename,
                                content: completion.choices[0].message.content,
                                transcript: transcription
                            }
                        )

                        if (titleAndSummaryGenerated.data.status !== 200) {
                            console.log('❌ Failed to save title/summary/transcript')
                        } else {
                            console.log('✅ Title, summary and transcript saved successfully')
                        }
                    }
                } else {
                    console.log('⚠️ File exceeds 25MB Groq Whisper limit, skipping transcription')
                }
            } catch (err) {
                console.error('❌ Transcription/summary error:', err.message)
            }
        } else {
            console.log('❌ S3 upload failed')
        }

        // 6. Mark processing as complete
        const stopProcessing = await axios.post(
            `${process.env.NEXT_API_HOST}recording/${data.userId}/complete`,
            { filename: data.filename }
        )
        if (stopProcessing.data.status !== 200) {
            console.log('🛑 Error marking video complete')
        }

        // Uncomment to clean up temp files after upload:
        // fs.unlink(rawPath, (err) => { if (!err) console.log('🟢 Raw temp deleted') })
        // fs.unlink(fixedPath, (err) => { if (!err) console.log('🟢 Fixed temp deleted') })
    })

    socket.on('disconnect', () => {
        console.log('🟢 Socket disconnected:', socket.id)
    })
})

server.listen(5000, () => {
    console.log('🟢 Listening on port 5000')
})