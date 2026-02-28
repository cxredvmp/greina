use super::*;
use dir::*;

pub struct File;

impl File {
    pub fn create(
        storage: &mut impl Storage,
        block_alloc: &mut impl block::Allocator,
        superblock: &mut Superblock,
        parent: NodeId,
        filetype: FileType,
        name: &str,
    ) -> Result<NodeId> {
        let name = DirEntryName::try_from(name)?;
        let id = Node::create(storage, block_alloc, superblock, filetype, 1)?;
        DirEntry::create(storage, block_alloc, superblock, parent, filetype, id, name)?;
        Ok(id)
    }

    pub fn read_at(
        storage: &impl Storage,
        superblock: &Superblock,
        id: NodeId,
        mut offset: u64,
        mut buf: &mut [u8],
    ) -> Result<u64> {
        let node = Node::read(storage, superblock, id)?;

        if offset >= node.size.get() {
            return Ok(0);
        };

        let avail = node.size.get() - offset;
        let to_read = avail.min(buf.len() as u64);
        buf = &mut buf[..to_read as usize];

        let mut read = 0;
        let mut block = Block::default();

        while !buf.is_empty() {
            if let Some(map) = MappedExtent::read(storage, superblock, id, offset)? {
                let avail_in_ext = map.end() - offset;
                let mut remain_in_ext = avail_in_ext.min(buf.len() as u64);

                let offset_in_ext = offset - map.start;
                let mut block_idx = offset_in_ext / BLOCK_SIZE;
                let mut offset_in_block = offset_in_ext % BLOCK_SIZE;

                while remain_in_ext != 0 {
                    let addr = map.inner.start() + block_idx;

                    let remain_in_block = BLOCK_SIZE - offset_in_block;
                    let chunk_size = remain_in_block.min(remain_in_ext);

                    storage.read_at(&mut block, addr)?;

                    let dst_end = chunk_size as usize;
                    let (dst, remain) = buf.split_at_mut(dst_end);

                    let src_start = offset_in_block as usize;
                    let src_end = src_start + chunk_size as usize;
                    let src = &block.data[src_start..src_end];

                    dst.copy_from_slice(src);

                    buf = remain;
                    read += chunk_size;
                    offset += chunk_size;
                    remain_in_ext -= chunk_size;

                    offset_in_block = 0;
                    block_idx += 1;
                }
            } else {
                let offset_in_block = offset % BLOCK_SIZE;
                let remain_in_block = BLOCK_SIZE - offset_in_block;
                let chunk_size = remain_in_block.min(buf.len() as u64);

                let dst_end = chunk_size as usize;
                let (dst, remain) = buf.split_at_mut(dst_end);

                dst.fill(0);

                buf = remain;
                read += chunk_size;
                offset += chunk_size;
            }
        }

        Ok(read)
    }

    pub fn write_at(
        storage: &mut impl Storage,
        block_alloc: &mut impl block::Allocator,
        superblock: &mut Superblock,
        id: NodeId,
        mut offset: u64,
        mut buf: &[u8],
    ) -> Result<u64> {
        let mut node = Node::read(storage, superblock, id)?;

        let mut written = 0;
        let mut block = Block::default();

        while !buf.is_empty() {
            let map = MappedExtent::ensure(
                storage,
                block_alloc,
                superblock,
                id,
                offset,
                buf.len() as u64,
            )?;

            let avail_in_ext = map.end() - offset;
            let mut remain_in_ext = avail_in_ext.min(buf.len() as u64);

            let offset_in_ext = offset - map.start;
            let mut block_idx = offset_in_ext / BLOCK_SIZE;
            let mut offset_in_block = offset_in_ext % BLOCK_SIZE;

            while remain_in_ext != 0 {
                let addr = map.inner.start() + block_idx;

                let remain_in_block = BLOCK_SIZE - offset_in_block;
                let chunk_size = remain_in_block.min(remain_in_ext);

                if chunk_size != BLOCK_SIZE {
                    storage.read_at(&mut block, addr)?;
                }

                let src_end = chunk_size as usize;
                let (src, remain) = buf.split_at(src_end);

                let dst_start = offset_in_block as usize;
                let dst_end = dst_start + chunk_size as usize;
                let dst = &mut block.data[dst_start..dst_end];

                dst.copy_from_slice(src);

                storage.write_at(&block, addr)?;

                buf = remain;
                written += chunk_size;
                offset += chunk_size;
                remain_in_ext -= chunk_size;

                offset_in_block = 0;
                block_idx += 1;
            }
        }

        if offset > node.size.get() {
            node.size.set(offset);
            node.write(storage, block_alloc, superblock, id)?;
        }

        Ok(written)
    }

    pub fn truncate(
        storage: &mut impl Storage,
        block_alloc: &mut impl block::Allocator,
        superblock: &mut Superblock,
        id: NodeId,
        size: u64,
    ) -> Result<()> {
        let mut node = Node::read(storage, superblock, id)?;

        if node.filetype != FileType::File {
            return Err(Error::NotFile);
        }

        if size < node.size.get() {
            Node::truncate_extents(storage, block_alloc, superblock, id, size)?;

            let remain = size % BLOCK_SIZE;
            if remain != 0 {
                if let Some(map) = MappedExtent::read(storage, superblock, id, size)? {
                    let addr = map.inner.start() + map.inner.len() - 1;
                    Self::zero_block_tail(storage, addr, remain)?;
                }
            }
        }

        node.size.set(size);
        node.write(storage, block_alloc, superblock, id)?;

        Ok(())
    }

    pub fn zero_block_tail(storage: &mut impl Storage, addr: BlockAddr, remain: u64) -> Result<()> {
        let mut block = Block::default();
        storage.read_at(&mut block, addr)?;

        let start = usize::try_from(remain).unwrap();
        (&mut block.data[start..]).fill(0);

        storage.write_at(&block, addr)?;

        Ok(())
    }
}
